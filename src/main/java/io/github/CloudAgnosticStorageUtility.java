/**
 * 
 */
package io.github;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * @author himanshu.kumar.sinha
 *
 */
public class CloudAgnosticStorageUtility {

	public static String moveObject(String cloud,
		      String projectId,
		      String sourceBucketName,
		      String sourceObjectName,
		      String targetBucketName,
		      String targetObjectName) throws Exception
	{
		// ############### Logic for GCP ###############
		if(cloud!=null && (cloud.equalsIgnoreCase("Google") || cloud.equalsIgnoreCase("GCP")))
		{
			System.out.println("Inside GCP Storage flow :::");
		    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
		    BlobId source = BlobId.of(sourceBucketName, sourceObjectName);
		    BlobId target = BlobId.of(targetBucketName, targetObjectName);
		    
		    Storage.BlobTargetOption precondition;
		    if (storage.get(targetBucketName, targetObjectName) == null) {
		      precondition = Storage.BlobTargetOption.doesNotExist();
		    } else {
		      precondition =
		          Storage.BlobTargetOption.generationMatch(
		              storage.get(targetBucketName, targetObjectName).getGeneration());
		    }
		    // Copy source object to target object
		    storage.copy(
		        Storage.CopyRequest.newBuilder().setSource(source).setTarget(target, precondition).build());
		    Blob copiedObject = storage.get(target);
		    storage.get(source).delete();
		    
		    return("Moved object "
		                + sourceObjectName
		                + " from bucket "
		                + sourceBucketName
		                + " to "
		                + targetObjectName
		                + " in bucket "
		                + copiedObject.getBucket());
	      }
		// ###############Logic for AWS ############### 
		else if (cloud!=null && (cloud.equalsIgnoreCase("Amazon") || cloud.equalsIgnoreCase("AWS"))) 
		{
			System.out.println("Inside AWS Storage flow :::");
			
	        CopyObjectRequest  copyReq = CopyObjectRequest.builder()
	                .sourceBucket(sourceBucketName)
	                .sourceKey(sourceObjectName)
	                .destinationBucket(targetBucketName)
	                .destinationKey(targetObjectName)
	                .build();
	            
	            S3Client s3=S3Client.builder().build();
	            try {
	                CopyObjectResponse copyRes = s3.copyObject(copyReq);
	                return copyRes.copyObjectResult().toString();

	            } catch (S3Exception e) {
	                System.err.println(e.awsErrorDetails().errorMessage());
	                System.exit(1);
	            }
			
		}
		throw new Exception("Only AWS and GCP currently supported.Please choose any one of them.");
	}
	
}
