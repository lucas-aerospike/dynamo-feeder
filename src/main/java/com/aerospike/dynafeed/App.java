package com.aerospike.dynafeed;


import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;


import java.time.Duration;
import java.util.Map;


public class App
{
    public static void main(String[ ] args) throws InterruptedException
    {
        var clientDriver = DynamoDbClient.builder()
            .region(Region.US_WEST_2)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build( );

        var client = DynamoDbEnhancedClient.builder( )
            .dynamoDbClient(clientDriver)
            .build( );

        var userSynthesizer = new UserSynthesizer(1);
        var userFeeder = new UserFeeder(client, userSynthesizer, 4,
            Duration.ofSeconds(1));

        var itemSynthesizer = new ItemSynthesizer(clientDriver);
        var itemFeeder = new ItemFeeder(client, itemSynthesizer, 4,
            Duration.ofMillis(200));

        userFeeder.start( );
        itemFeeder.start( );

        // a thread self-join means "wait forever"
        Thread.currentThread( ).join( );
    }
}
