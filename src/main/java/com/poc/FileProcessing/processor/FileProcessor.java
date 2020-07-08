package com.poc.FileProcessing.processor;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import com.poc.FileProcessing.entities.Student;
import com.poc.FileProcessing.repository.StudentRepository;

public class FileProcessor {
	
	@Autowired
	private StudentRepository studentRepository;
	
    private static final String HEADER_FILE_NAME = "file_name";
    private static final String MSG = "%s received";

    public void process(Message<File> msg) {
    	String fileName = (String) msg.getHeaders().get(HEADER_FILE_NAME);
        File file = msg.getPayload();
        System.out.println(String.format(MSG, fileName));
        try {
			LineIterator it = FileUtils.lineIterator(file, "UTF-8");
			List<Student> students = new ArrayList<Student>();
			while (it.hasNext()) {
				String line = it.nextLine();
				String data[] = line.split("\\s+");
				Student student = new Student();
				student.setName(data[0]);
				student.setSubject(data[1]);
				students.add(student);
			}
			System.out.println(students.size());
			Instant start = Instant.now();
			studentRepository.saveAll(students);
			Instant end = Instant.now();
			System.out.println("Time taken to process "+ students.size()+ " data is " + Duration.between(start, end).toMillis()+ " milli seconds");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
    }

}
