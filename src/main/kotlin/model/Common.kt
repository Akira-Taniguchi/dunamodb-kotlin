package model

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model.*
import java.util.*


private interface TableName {
    val tableName: String
}

abstract class DynamoDBBase : TableName {
    val client = getDynamoDbClient()

    protected fun deleteItemBase(keyMap: Map<String, AttributeValue>): DeleteItemResult {
        val request = DeleteItemRequest().withTableName(tableName).withKey(keyMap)
        return client.deleteItem(request)
    }

    protected fun getItemBase(keyMap: Map<String, AttributeValue>): Map<String, AttributeValue> {
        val request = GetItemRequest().withTableName(tableName).withKey(keyMap)
        val result = client.getItem(request)
        if (result.item != null) {
            return result.item
        }
        return emptyMap()
    }

    protected fun putItemBase(itemMap: Map<String, AttributeValue>) {
        val request = PutItemRequest().withTableName(tableName).withItem(itemMap)
        client.putItem(request)
    }

    protected fun query(keyConditions: HashMap<String, Condition>): List<Map<String, AttributeValue>> {
        val queryRequest = QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(keyConditions)
        val queryResult = client.query(queryRequest)
        return queryResult.items
    }

    protected fun batchGetItem(conditions: List<Map<String, AttributeValue>>): List<Map<String, AttributeValue>>? {
        val request = BatchGetItemRequest()
        val keys = KeysAndAttributes().withKeys(conditions)
        request.addRequestItemsEntry(tableName, keys)
        val result = client.batchGetItem(request)
        return result.responses[tableName]
    }
}

internal fun getDynamoDbClient(): AmazonDynamoDB {
    val serviceEndPoint = System.getenv("DYNAMODB_SERVICE_END_POINT") ?: "http://localhost:8000"
    val region = System.getenv("DYNAMODB_SERVICE_REGION") ?: ""
    var clientBuilder = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration(serviceEndPoint, region))
    val env = System.getenv("ENV") ?: ""
    if (env == "") {
        clientBuilder = clientBuilder.withCredentials(
                AWSStaticCredentialsProvider(
                        BasicAWSCredentials("", "")
                )
        )
    }
    return clientBuilder.build()
}
