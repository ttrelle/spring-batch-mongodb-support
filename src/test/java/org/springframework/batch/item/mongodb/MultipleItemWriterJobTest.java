package org.springframework.batch.item.mongodb;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.UnknownHostException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Job test with multiple writers {@link MongoDBItemWriter}.
 * <p/>
 * This test assumes that a mongod instance is running on localhost at the default port 27017. 
 * If you want to use other values, use VM parameters -Dhost=... and -Dport=...
 * 
 * @author Tobias Trelle
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class MultipleItemWriterJobTest extends AbstractMongoDBTest {
	
	@Autowired private JobLauncher launcher;
	
	@Autowired private Job job;
	
    @Before public void setUp() throws UnknownHostException {
    	setUpMongo();
    }	

    /**
     * If using a composite item writer (here: MongoDB + JDBC), the MongoDB writer should not write 
     * any documents if one of the other writers causes a rollback.
     * 
     * This only works if the {@link MongoDBItemWriter} is configured to run in 
     * pseudo-transactional mode (default).
     * 
     * @throws IOException
     */
    @Test
    public void should_write_no_documents_if_another_tx_resource_rolls_back() throws IOException {

        // given
        JobParametersBuilder paramBuilder = new JobParametersBuilder();
        paramBuilder.addString("db", DB_NAME);
        paramBuilder.addString("collection", COLLECTION_NAME);
        paramBuilder.addString("inputfile", "classpath:org/springframework/batch/item/mongodb/input.json");
        paramBuilder.addString("transactional", "true");
        
        try {
            // when ...
            JobExecution execution = launcher.run(job, paramBuilder.toJobParameters());

            // then ...
            assertThat(execution.getExitStatus(), is(ExitStatus.FAILED) );
            assertCollectionCount(0);

        } catch (JobExecutionException e) {
            fail("Job execution failed");
        }
    }

    /**
     * If using a composite item writer (here: MongoDB + JDBC), the MongoDB writer may write 
     * a chunk documents even if one of the other writers causes a rollback.
     * 
     * This happens if the {@link MongoDBItemWriter} is not configured to run in 
     * pseudo-transactional mode and is used to write before one of the other writers fails.
     * 
     * @throws IOException
     */
    @Test
    public void should_write_documents_although_another_tx_resource_rolls_back() throws IOException {

        // given
        JobParametersBuilder paramBuilder = new JobParametersBuilder();
        paramBuilder.addString("db", DB_NAME);
        paramBuilder.addString("collection", COLLECTION_NAME);
        paramBuilder.addString("inputfile", "classpath:org/springframework/batch/item/mongodb/input.json");
        paramBuilder.addString("transactional", "false");
        
        try {
            // when ...
            JobExecution execution = launcher.run(job, paramBuilder.toJobParameters());

            // then ...
            assertThat(execution.getExitStatus(), is(ExitStatus.FAILED) );
            assertCollectionCount(3);

        } catch (JobExecutionException e) {
            fail("Job execution failed");
        }
    }
    
    @After public void tearDown() {
    	tearDownMongo();
    }	
	
}
