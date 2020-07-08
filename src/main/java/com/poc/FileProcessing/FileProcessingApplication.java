package com.poc.FileProcessing;

import java.io.File;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.FileWritingMessageHandler;
import org.springframework.integration.file.filters.CompositeFileListFilter;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.file.support.FileExistsMode;
import org.springframework.integration.file.transformer.FileToStringTransformer;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.poc.FileProcessing.filter.LastModifiedFileFilter;
import com.poc.FileProcessing.processor.FileProcessor;

@SpringBootApplication
@EnableScheduling
public class FileProcessingApplication {

	@Value("${polling.folder.path}")
	private String DIRECTORY;
	
	@Value("${processed.folder.path}")
	private String PROCESSED_DIR;
	
	public static void main(String[] args) {
		SpringApplication.run(FileProcessingApplication.class, args);
	}

	@Bean
	public IntegrationFlow processFileFlow() {
		return IntegrationFlows
				.from("outputChannel")
				.handle("fileProcessor", "process")
                .get();
	}
	
    @Bean
    public MessageChannel fileInputChannel() {
        return new DirectChannel();
    }
    
    @Bean
    public MessageChannel outputChannel() {
        return new DirectChannel();
    }

	@Bean
	@InboundChannelAdapter(value = "fileInputChannel", poller = @Poller(fixedDelay = "1000"))
	public MessageSource<File> fileReadingMessageSource() {
		CompositeFileListFilter<File> filters =new CompositeFileListFilter<>();
		filters.addFilter(new SimplePatternFileListFilter("*.txt"));
		filters.addFilter(new LastModifiedFileFilter());

		FileReadingMessageSource source = new FileReadingMessageSource();
		source.setAutoCreateDirectory(true);
		source.setDirectory(new File(DIRECTORY));
		source.setFilter(filters);
		
		return source;
	}

	@Bean
	public FileToStringTransformer fileToStringTransformer() {
		return new FileToStringTransformer();
	}

	@Bean
	public FileProcessor fileProcessor() {
		return new FileProcessor();
	}
	
    @Bean
    @ServiceActivator(inputChannel = "fileInputChannel")
    public MessageHandler fileOutboundgateway() {

        FileWritingMessageHandler gateway = new FileWritingMessageHandler(new File(PROCESSED_DIR));
        gateway.setDeleteSourceFiles(true);
        gateway.setFileExistsMode(FileExistsMode.REPLACE);
        gateway.setAutoCreateDirectory(true);
        gateway.setOutputChannel(outputChannel());
        return gateway;
    }
    
}
