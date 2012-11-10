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

import com.mongodb.BasicDBObject;

/**
 * Integration tests for the {@link MongoDBItemWriter}.
 * <p/>
 * This test assumes that a mongod instance is running on localhost at the default port 27017. 
 * If you want to use other values, use VM parameters -Dhost=... and -Dport=...
 * 
 * @author Tobias Trelle
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class MongoDBItemWriterJobTest extends AbstractMongoDBTest {
	
	@Autowired private JobLauncher launcher;
	
	@Autowired private Job job;
	
    @Before public void setUp() throws UnknownHostException {
    	setUpMongo();
    }	

    @Test
    public void should_write_to_collection() throws IOException {

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
            assertThat(execution.getExitStatus(), is(ExitStatus.COMPLETED) );
            assertCollectionCount(5);

        } catch (JobExecutionException e) {
            fail("Job execution failed");
        }
    }
    
    
    @Test
    public void should_write_to_collection_transactional() throws IOException {

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
            assertThat(execution.getExitStatus(), is(ExitStatus.COMPLETED) );
            assertCollectionCount(5);

        } catch (JobExecutionException e) {
            fail("Job execution failed");
        }
    }

    @Test
    public void should_fail_after_first_committed_chunk() throws IOException {

        // given
        JobParametersBuilder paramBuilder = new JobParametersBuilder();
        paramBuilder.addString("db", DB_NAME);
        paramBuilder.addString("collection", COLLECTION_NAME);
        paramBuilder.addString("inputfile", "classpath:org/springframework/batch/item/mongodb/input-indexviolation.json");
        paramBuilder.addString("transactional", "false");
        collection.ensureIndex( new BasicDBObject("a", 1) , "a_1", true);
        
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
    
    @Test
    public void should_fail_after_first_committed_chunk_transactional() throws IOException {

        // given
        JobParametersBuilder paramBuilder = new JobParametersBuilder();
        paramBuilder.addString("db", DB_NAME);
        paramBuilder.addString("collection", COLLECTION_NAME);
        paramBuilder.addString("inputfile", "classpath:org/springframework/batch/item/mongodb/input-indexviolation.json");
        paramBuilder.addString("transactional", "true");
        collection.ensureIndex( new BasicDBObject("a", 1) , "a_1", true);
        
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
