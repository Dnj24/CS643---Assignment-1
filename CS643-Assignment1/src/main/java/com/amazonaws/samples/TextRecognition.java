package com.amazonaws.samples;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.DetectLabelsRequest;
import com.amazonaws.services.rekognition.model.DetectTextRequest;
import com.amazonaws.services.rekognition.model.DetectTextResult;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.rekognition.model.TextDetection;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sqs.model.*;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazon.sqs.javamessaging.AmazonSQSExtendedClient;
import com.amazon.sqs.javamessaging.ExtendedClientConfiguration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
/* SOURCES USED: 
 * https://stackoverflow.com/questions/31027271/aws-sqs-java-not-all-messages-are-retrieved-from-the-sqs-queue
 * https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-s3-messages.html
 * */
import java.io.IOException;

public class TextRecognition {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		BufferedWriter bw = null;

		try {
			bw = new BufferedWriter(new FileWriter("TextRecognition.txt"));

		} catch (IOException e) {
			e.printStackTrace();
		}

		try {

			AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
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

			/* List s3 objects */
			ObjectListing objectListing = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName));
			DetectLabelsRequest request = null;
			AmazonRekognition rekognitionClient = null;

			boolean flag = true;

			while (flag) {
				// Receive the message.
				final ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
						"https://sqs.us-east-1.amazonaws.com/025615080222/ImageRecognition");

				List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

				// Print information about the message from sqs.
				for (Message message : messages) {

					// System.out.println("message body");
					// System.out.println(message.getBody());

					// s3 objects
					for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
						/* Call Rekognition API */
						rekognitionClient = AmazonRekognitionClientBuilder.standard().withRegion(Regions.US_EAST_1)
								.build();

						request = new DetectLabelsRequest()
								.withImage(new Image().withS3Object(
										new S3Object().withName(objectSummary.getKey()).withBucket(bucketName)))
								.withMaxLabels(10).withMinConfidence(80F);

						// System.out.println("Request: " + request);

						if (message.getBody().equalsIgnoreCase(request.getImage().getS3Object().getName())) {

							DetectTextRequest textRequest = new DetectTextRequest().withImage(new Image()
									.withS3Object(new S3Object().withName(message.getBody()).withBucket(bucketName)));

							try {

								// Download the image from s3
								com.amazonaws.services.s3.model.S3Object o = s3.getObject(bucketName,
										message.getBody());
								S3ObjectInputStream s3is = o.getObjectContent();
								FileOutputStream fos = new FileOutputStream(new File(message.getBody()));
								byte[] read_buf = new byte[1024];
								int read_len = 0;
								while ((read_len = s3is.read(read_buf)) > 0) {
									fos.write(read_buf, 0, read_len);
								}
								s3is.close();
								fos.close();

								// Text detection
								DetectTextResult result = rekognitionClient.detectText(textRequest);
								// System.out.println(result);
								List<TextDetection> textDetections = result.getTextDetections();

								for (TextDetection text : textDetections) {

									try {

										System.out.println(
												"Detected: " + message.getBody() + " " + text.getDetectedText());
										bw.write(
												"Detected: " + message.getBody() + " " + text.getDetectedText() + "\n");
										// bw.close();
										// myWriter.write("Detected: " + message.getBody() + " " +
										// text.getDetectedText());

									} catch (IOException e) {
										System.out.println("An error occurred.");
										e.printStackTrace();
									} finally {
										// bw.close();
									}

								}

							} catch (AmazonServiceException e) {
								System.err.println(e.getErrorMessage());
								System.exit(1);
							} catch (FileNotFoundException e) {
								System.err.println(e.getMessage());
								System.exit(1);
							} catch (IOException e) {
								System.err.println(e.getMessage());
								System.exit(1);
							}

							String messageReceiptHandle = message.getReceiptHandle();
							sqs.deleteMessage(new DeleteMessageRequest()
									.withQueueUrl("https://sqs.us-east-1.amazonaws.com/025615080222/ImageRecognition")
									.withReceiptHandle(messageReceiptHandle));

						}

					}

				}

				if (messages.size() == 0) {
					flag = false;

				}
			}

		} catch (AmazonServiceException ase) {
			ase.printStackTrace();
		} catch (AmazonClientException ace) {
			ace.printStackTrace();
		} finally {
			bw.close();
		}

	}

}
