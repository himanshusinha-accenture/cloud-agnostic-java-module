/**
 * 
 */
package io.github;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * @author himanshu.kumar.sinha
 *
 */
public class AwsStorageFunctionUtility {
    public static String copyBucketObject(String fromBucket, String objectKey, String toBucket) {

        CopyObjectRequest  copyReq = CopyObjectRequest.builder()
            .sourceBucket(fromBucket)
            .sourceKey(objectKey)
            .destinationBucket(toBucket)
            .destinationKey(objectKey)
            .build();
        
        S3Client s3=S3Client.builder().build();
        try {
            CopyObjectResponse copyRes = s3.copyObject(copyReq);
            return copyRes.copyObjectResult().toString();

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "success";
    }

    public static void moveObject(
		      String projectId,
		      String sourceBucketName,
		      String sourceObjectName,
		      String targetBucketName,
		      String targetObjectName) {
    	
    	//TODO 
    }
  
    public static void listBuckets(String projectId) 
    {
    	//TODO
    }
}
