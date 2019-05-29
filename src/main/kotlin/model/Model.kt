package model

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator
import com.amazonaws.services.dynamodbv2.model.Condition
import com.amazonaws.services.dynamodbv2.model.ScanRequest


data class HogeTableRecord(val idHash: Int,
                           val valRange: String,
                           val hogeName: String,
                           val hogeDetail: String)


class HogeTable : DynamoDBBase() {
    override val tableName = "hogehoge_table"
    val idName = "hogehoge_id"

    fun hasRecord(id: Int): Boolean {
        return getItem(id) != null
    }

    fun getItem(id: Int): HogeTableRecord? {
        //ハッシュキーによるレコード取得
        val keyMap = mutableMapOf<String, AttributeValue>()
        keyMap[idName] = AttributeValue().withN(id.toString())
        val item = getItemBase(keyMap)
        if (item.isEmpty()) {
            return null
        }
        return HogeTableRecord(item["id"]?.n!!.toInt(),
                item["val_range"]?.s!!,
                item["hoge_name"]?.s!!,
                item["hoge_detail"]?.s!!)
    }

    fun getItems(ids: List<Int>): List<HogeTableRecord> {
        //ハッシュキーによる複数レコード取得
        val conditions = ids.map {
            mapOf(idName to AttributeValue().withN(it.toString()))
        }
        val data = batchGetItem(conditions)?.map { item ->
            HogeTableRecord(item["id"]?.n!!.toInt(),
                    item["val_range"]?.s!!,
                    item["hoge_name"]?.s!!,
                    item["hoge_detail"]?.s!!)
        }
        return if (data!!.isEmpty()) emptyList() else data
    }

    fun getAllItems(): List<HogeTableRecord> {
        //全レコード取得
        val response = client.scan(ScanRequest().withTableName(tableName))
        val data = response.items.map { item ->
            HogeTableRecord(item["id"]?.n!!.toInt(),
                    item["val_range"]?.s!!,
                    item["hoge_name"]?.s!!,
                    item["hoge_detail"]?.s!!)
        }
        return if (data.isEmpty()) emptyList() else data
    }

    fun putItem(id: Int, range: String, name: String, detail: String): HogeTableRecord {
        //レコード登録
        val record = HogeTableRecord(id, range, name, detail)
        val itemAttrMap = mapOf(
                "id" to AttributeValue().withN(record.idHash.toString()),
                "hoge_name" to AttributeValue().withS(record.hogeName),
                "hoge_detail" to AttributeValue().withS(record.hogeDetail)
        )
        putItemBase(itemAttrMap)
        return record
    }

    fun deleteItem(id: Int) {
        //ハッシュキーによるレコード削除
        val keyMap = mutableMapOf<String, AttributeValue>()
        keyMap["id"] = AttributeValue().withN(id.toString())
        deleteItemBase(keyMap)
    }

    fun getItemsByUserId(valrange: String): List<HogeTableRecord> {
        //レンジキーによる複数行取得
        val keyConditions = HashMap<String, Condition>()
        keyConditions["val_range"] = Condition()
                .withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(AttributeValue().withS(valrange))
        return query(keyConditions).map { item ->
            HogeTableRecord(item["id"]?.n!!.toInt(),
                    item["val_range"]?.s!!,
                    item["hoge_name"]?.s!!,
                    item["hoge_detail"]?.s!!)
        }
    }
}