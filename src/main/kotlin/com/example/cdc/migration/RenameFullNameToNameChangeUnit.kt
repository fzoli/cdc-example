package com.example.cdc.migration

import io.mongock.api.annotations.ChangeUnit
import io.mongock.api.annotations.Execution
import io.mongock.api.annotations.RollbackExecution
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update

@ChangeUnit(id = "rename-name-to-fullName", order = "001", author = "zoltan")
class RenameNameToFullNameChangeUnit {

    @Execution
    fun migrate(mongoTemplate: MongoTemplate) {
        val query = Query(Criteria.where("name").exists(true))
        val update = Update().rename("name", "fullName")
        val result = mongoTemplate.updateMulti(query, update, "devices")
        println("Renamed field in ${result.modifiedCount} documents")
    }

    @RollbackExecution
    fun rollback(mongoTemplate: MongoTemplate) {
        val query = Query(Criteria.where("fullName").exists(true))
        val update = Update().rename("fullName", "name")
        mongoTemplate.updateMulti(query, update, "devices")
        println("Rollback executed — restored original field name")
    }

}
