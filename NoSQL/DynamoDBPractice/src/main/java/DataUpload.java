import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class DataUpload {

    static final String BUCKET_NAME = "14848-experiment-data";
    static final String TABLE_NAME   = "DataTable";
    static final String FILE_PATH_PREFIX = "/Users/haileypan/Desktop/Cloud Infra/homework3/DynamoDBPractice/data/";

    public static void createS3Bucket(S3Client s3) {
        try {
            S3Waiter s3Waiter = s3.waiter();
            CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                    .bucket(BUCKET_NAME)
                    .acl("public-read")
                    .build();

            s3.createBucket(bucketRequest);
            HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                    .bucket(BUCKET_NAME)
                    .build();

            // Wait until the bucket is created and print out the response.
            s3Waiter.waitUntilBucketExists(bucketRequestWait);
            System.out.println("Bucket " + BUCKET_NAME +" is ready");
        } catch (S3Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void uploadS3Object(S3Client s3, String objectKey, String filePath) {
        try {
            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(objectKey)
                    .acl("public-read")
                    .build();

            PutObjectResponse response = s3.putObject(putOb, RequestBody.fromBytes(getObjectFile(filePath)));
            System.out.println("Object uploaded: " + objectKey);
        } catch (S3Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static byte[] getObjectFile(String filePath) {

        FileInputStream fileInputStream = null;
        byte[] bytesArray = null;

        try {
            File file = new File(filePath);
            bytesArray = new byte[(int) file.length()];
            fileInputStream = new FileInputStream(file);
            fileInputStream.read(bytesArray);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return bytesArray;
    }

    public static void createTable(DynamoDbClient dynamodb) {
        DynamoDbWaiter  dynamoDbWaiter = dynamodb.waiter();
        CreateTableRequest request = CreateTableRequest.builder()
                .attributeDefinitions(
                        AttributeDefinition.builder()
                                .attributeName("PartitionKey")
                                .attributeType(ScalarAttributeType.S)
                                .build(),
                        AttributeDefinition.builder()
                                .attributeName("RowKey")
                                .attributeType(ScalarAttributeType.S)
                                .build())
                .keySchema(
                        KeySchemaElement.builder()
                                .attributeName("PartitionKey")
                                .keyType(KeyType.HASH)
                                .build(),
                        KeySchemaElement.builder()
                                .attributeName("RowKey")
                                .keyType(KeyType.RANGE)
                                .build())
                .provisionedThroughput(
                        ProvisionedThroughput.builder()
                                .readCapacityUnits(new Long(10))
                                .writeCapacityUnits(new Long(10)).build())
                .tableName(TABLE_NAME)
                .build();

        try {
            dynamodb.createTable(request);
            DescribeTableRequest describeTableRequest = DescribeTableRequest.builder()
                    .tableName(TABLE_NAME)
                    .build();
            dynamoDbWaiter.waitUntilTableExists(describeTableRequest);
        } catch (DynamoDbException e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("Table created " + TABLE_NAME );
    }

    public static void putItemInTable(DynamoDbClient dynamodb, List<String> fields, List<String> values) {
        HashMap<String, AttributeValue> itemValues = new HashMap();
        // Add all content to the table
        for (int i = 0; i < fields.size(); i++) {
            itemValues.put(fields.get(i), AttributeValue.builder().s(values.get(i)).build());
        }

        PutItemRequest request = PutItemRequest.builder()
                .tableName(TABLE_NAME)
                .item(itemValues)
                .build();

        try {
            dynamodb.putItem(request);
            System.out.println("Item was successfully inserted");

        } catch (ResourceNotFoundException e) {
            System.err.format("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", TABLE_NAME);
            System.err.println("Be sure that it exists and that you've typed its name correctly!");
            System.exit(1);
        } catch (DynamoDbException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

   public static void getItem(DynamoDbClient dynamodb) {

       HashMap<String,AttributeValue> keyToGet = new HashMap();

       keyToGet.put("RowKey", AttributeValue.builder()
               .s("experiment2").build());

       keyToGet.put("PartitionKey", AttributeValue.builder()
               .s("2").build());

       GetItemRequest request = GetItemRequest.builder()
               .key(keyToGet)
               .tableName(TABLE_NAME)
               .build();

       try {
           GetItemResponse getItemResponse = dynamodb.getItem(request);
           System.out.println(getItemResponse);
       } catch (DynamoDbException e) {
           e.printStackTrace();
           System.exit(1);
       }
   }

   public static void loadData(S3Client s3, DynamoDbClient dynamodb) {
        String masterDataPath = FILE_PATH_PREFIX + "/experiments.csv";
        File file= new File(masterDataPath);

       // this gives you a 2-dimensional array of strings
       List<List<String>> lines = new ArrayList<>();
       Scanner inputStream;

       try{
           inputStream = new Scanner(file);

           while(inputStream.hasNext()){
               String line= inputStream.next();
               String[] values = line.split(",");
               lines.add(new ArrayList<>(Arrays.asList(values)));
           }
           inputStream.close();

           List<String> header = lines.get(0);
           header.set(0, "PartitionKey");
           header.add("RowKey");
           for (int i = 1; i < lines.size(); i++) {
               List<String> line = lines.get(i);
               String url = line.get(4);
               // upload files to s3
               uploadS3Object(s3, url, FILE_PATH_PREFIX + url);
               line.set(4, "https://" + BUCKET_NAME + ".s3.amazonaws.com/" + url);
               // add RowKey
               line.add("experiment" + line.get(0));

               putItemInTable(dynamodb, header, lines.get(i));
           }
       }catch (FileNotFoundException e) {
           e.printStackTrace();
       }
   }

    public static void main(final String[] args) {
        // Remember to set environment variables AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
        EnvironmentVariableCredentialsProvider credentials = EnvironmentVariableCredentialsProvider.create();

        S3Client s3 = S3Client.builder()
                .credentialsProvider(credentials)
                .region(Region.US_EAST_1)
                .build();

        DynamoDbClient dynamodb = DynamoDbClient.builder()
                .credentialsProvider(credentials)
                .region(Region.US_EAST_1)
                .build();

        createS3Bucket(s3);

        createTable(dynamodb);

        loadData(s3, dynamodb);

        getItem(dynamodb);

        System.exit(0);
    }
}
