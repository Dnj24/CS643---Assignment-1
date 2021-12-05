package com.amazonaws.samples;
/*
 * Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.AmazonRekognitionException;
import com.amazonaws.services.rekognition.model.DetectLabelsRequest;
import com.amazonaws.services.rekognition.model.DetectLabelsResult;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.Label;
import com.amazonaws.services.rekognition.model.S3Object;


import java.io.IOException;
import java.util.List;
import java.util.UUID;

/* SOURCES USED: 
 * 
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 * https://docs.aws.amazon.com/rekognition/latest/dg/images-s3.html
 * https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-s3-messages.html
 * https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/examples-s3-buckets.html
 * */


public class ImageRecognition {

	public static void main(String[] args) throws IOException {
		AmazonS3 s3 = null;
		try {
			
			/* Load credentials from Preferences -> AWS Toolkit */
			AWSCredentials credentials = new ProfileCredentialsProvider("default").getCredentials();
			s3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials))
					.withRegion(Regions.US_EAST_1).build();

		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. ", e);
		}

		String bucketName = "njit-cs-643";

		try {
			
			
			AmazonSQS sqs = AmazonSQSClientBuilder.standard()
					.withRegion(Regions.US_EAST_1)
					.build();
			
			// Create a SQS message queue
		    final String QueueName = "ImageRecognition";
		    final CreateQueueRequest createQueueRequest =
		            new CreateQueueRequest(QueueName);
		    final String myQueueUrl = sqs
		            .createQueue(createQueueRequest).getQueueUrl();
		    System.out.println("Queue created: " + myQueueUrl);

		    /* List s3 objects */
			ObjectListing objectListing = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName));
			
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {

				/* Call Rekognition API */
				AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.standard()
						.withRegion(Regions.US_EAST_1)
						.build();

				DetectLabelsRequest request = new DetectLabelsRequest()
						.withImage(new Image().withS3Object(new S3Object().withName(objectSummary.getKey()).withBucket(bucketName)))
						.withMaxLabels(10).withMinConfidence(80F);

				System.out.println("Request: " + request);
				
				try {
					
					DetectLabelsResult result = rekognitionClient.detectLabels(request);
					//System.out.println("Result" + result);
				    
					List<Label> labels = result.getLabels();
					
					for (Label label : labels) {
						if(label.getName().equalsIgnoreCase("Car") && label.getConfidence() > 80) {
							
							//Print out the file name that has confidence >80% and 'Car' label
							System.out.println(request.getImage().getS3Object().getName());
							   
						    //Send the message to SQS
						    final SendMessageRequest myMessageRequest =
						            new SendMessageRequest(myQueueUrl, request.getImage().getS3Object().getName());
						    sqs.sendMessage(myMessageRequest);
						    System.out.println("Sent the message.");
							
						}
					}
				} catch (AmazonRekognitionException e) {
					e.printStackTrace();
				}
			}

		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}

		
	}
}



