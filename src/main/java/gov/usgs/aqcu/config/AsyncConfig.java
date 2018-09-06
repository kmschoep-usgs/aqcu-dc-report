package gov.usgs.aqcu.config;

import java.util.concurrent.Executor;

import javax.validation.ValidationException;
import javax.validation.constraints.Min;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableAsync
public class AsyncConfig {
	 /**
	  * Executor Size Rules (taken from bigsoft.co.uk/blog/2009/11/27/rules-of-a-threadpoolexecturo-pools-ize)
	  * 
	  * 1. If the number of threads is < corePoolSize then create a new Thread to run the new task.
	  * 2. If the number of threads is >= corePoolSize then put the new task into the queue.
	  * 3. If the queue is full and the number of threads < maxPoolSize then create a new thread to run tasks from the queue.
	  * 4. If the queue is full and the number of threads is >= maxPoolSize then reject any new tasks.
	  * 
	  * TLDR; New threads are only created when the queue fills up so if you're using an unbounded queue size (-1) then the
	  * number of threads will never exceed the corePoolSize. 
	  */

	 @Value("${asyncRetrieval.corePoolSize:10}")
	 @Min(1)
	 Integer corePoolSize;

	 @Value("${asyncRetrieval.maxPoolSize:-1}")
	 Integer maxPoolSize;

	 @Value("${asyncRetrieval.queueCapacity:-1}")
	 Integer queueCapacity;

	 @Bean(name = "derivationChainRetrievalExecutor")
	 public Executor derivationChainRetrievalExecutor() {
		  //Validate Configuration
		  if(maxPoolSize == -1) {
				maxPoolSize = Integer.MAX_VALUE;
		  } else if(maxPoolSize <= 0) {
				throw new ValidationException("Max Pool Size must be greater than 0 or equal to -1.");
		  }

		  if(queueCapacity == -1) {
				queueCapacity = Integer.MAX_VALUE;
		  } else if(queueCapacity <= 0) {
				throw new ValidationException("Queue Capacity must be greater than 0 or equal to -1.");
		  }

		  //Create Executor
		  ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		  executor.setCorePoolSize(corePoolSize);
		  executor.setMaxPoolSize(maxPoolSize);
		  executor.setQueueCapacity(queueCapacity);
		  executor.setThreadNamePrefix("DerivationChainRetrieval-");
		  executor.initialize();
		  return executor;
	 }
}