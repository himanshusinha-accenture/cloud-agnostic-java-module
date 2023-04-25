/**
 * 
 */
package io.github;

import java.nio.file.Paths;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

/**
 * @author himanshu.kumar.sinha
 *
 */
public class GcpStorageFunctionUtility {
	public static void moveObject(
		      String projectId,
		      String sourceBucketName,
		      String sourceObjectName,
		      String targetBucketName,
		      String targetObjectName)
	{
	    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
	    BlobId source = BlobId.of(sourceBucketName, sourceObjectName);
	    BlobId target = BlobId.of(targetBucketName, targetObjectName);
	    
	    Storage.BlobTargetOption precondition;
	    if (storage.get(targetBucketName, targetObjectName) == null) {
	      // For a target object that does not yet exist, set the DoesNotExist precondition.
	      // This will cause the request to fail if the object is created before the request runs.
	      precondition = Storage.BlobTargetOption.doesNotExist();
	    } else {
	      // If the destination already exists in your bucket, instead set a generation-match
	      // precondition. This will cause the request to fail if the existing object's generation
	      // changes before the request runs.
	      precondition =
	          Storage.BlobTargetOption.generationMatch(
	              storage.get(targetBucketName, targetObjectName).getGeneration());
	    }

	    // Copy source object to target object
	    storage.copy(
	        Storage.CopyRequest.newBuilder().setSource(source).setTarget(target, precondition).build());
	    Blob copiedObject = storage.get(target);
	    // Delete the original blob now that we've copied to where we want it, finishing the "move"
	    // operation
	    storage.get(source).delete();
	    
	    System.out.println(
	            "Moved object "
	                + sourceObjectName
	                + " from bucket "
	                + sourceBucketName
	                + " to "
	                + targetObjectName
	                + " in bucket "
	                + copiedObject.getBucket());
	      }
	
	public static void listBuckets(String projectId) {
	    // The ID of your GCP project
	    // String projectId = "your-project-id";

	    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
	    Page<Bucket> buckets = storage.list();

	    for (Bucket bucket : buckets.iterateAll()) {
	      System.out.println(bucket.getName());
	    }
	}
	
	  public static void downloadObject(
		      String projectId, String bucketName, String objectName, String destFilePath) {
		    // The ID of your GCP project
		    // String projectId = "your-project-id";

		    // The ID of your GCS bucket
		    // String bucketName = "your-unique-bucket-name";

		    // The ID of your GCS object
		    // String objectName = "your-object-name";

		    // The path to which the file should be downloaded
		    // String destFilePath = "/local/path/to/file.txt";

		    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();

		    Blob blob = storage.get(BlobId.of(bucketName, objectName));
		    blob.downloadTo(Paths.get(destFilePath));

		    System.out.println(
		        "Downloaded object "
		            + objectName
		            + " from bucket name "
		            + bucketName
		            + " to "
		            + destFilePath);
		  }
	  
	  public static void deleteObject(String projectId, String bucketName, String objectName) {
		    // The ID of your GCP project
		    // String projectId = "your-project-id";

		    // The ID of your GCS bucket
		    // String bucketName = "your-unique-bucket-name";

		    // The ID of your GCS object
		    // String objectName = "your-object-name";

		    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
		    Blob blob = storage.get(bucketName, objectName);
		    if (blob == null) {
		      System.out.println("The object " + objectName + " wasn't found in " + bucketName);
		      return;
		    }

		    Storage.BlobSourceOption precondition =
		        Storage.BlobSourceOption.generationMatch(blob.getGeneration());

		    storage.delete(bucketName, objectName, precondition);

		    System.out.println("Object " + objectName + " was deleted from " + bucketName);
		  }	  
	  
	  
	  public static void deleteBucket(String projectId, String bucketName) {
		    // The ID of your GCP project
		    // String projectId = "your-project-id";

		    // The ID of the bucket to delete
		    // String bucketName = "your-unique-bucket-name";

		    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
		    Bucket bucket = storage.get(bucketName);
		    bucket.delete();

		    System.out.println("Bucket " + bucket.getName() + " was deleted");
		  }
}
