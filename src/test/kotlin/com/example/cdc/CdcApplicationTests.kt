package com.example.cdc

import com.example.cdc.service.message.MessageRequestDto
import com.example.cdc.service.message.MessageService
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.event.RecordApplicationEvents

@Import(TestcontainersConfiguration::class)
@SpringBootTest
@RecordApplicationEvents
class CdcApplicationTests @Autowired constructor(
    private val messageService: MessageService,
    private val messageEventTestListener: MessageEventTestListener,
) {

	@Test
	fun `message events received`() {
        val createRequest = MessageRequestDto(message = "Hello World!", username = "Test User")
        val created = messageService.upsertMessage(createRequest)
        assertThat(created.id).isNotNull()
        assertThat(created.createTime).isEqualTo(created.updateTime)
        val id = created.id!!

        val firstMessage = messageEventTestListener.awaitMessageById(id)
        assertThat(firstMessage).isEqualTo(created)

        messageEventTestListener.removeMessageById(id)

        val updateRequest = createRequest.copy(id = id, message = "Hello Again!")
        val updated = messageService.upsertMessage(updateRequest)
        assertThat(updated.id).isEqualTo(created.id)
        assertThat(updated.createTime).isEqualTo(created.createTime)
        assertThat(updated.updateTime).isNotEqualTo(updated.createTime)

        val secondMessage = messageEventTestListener.awaitMessageById(id)
        assertThat(secondMessage).isEqualTo(updated)

        messageService.deleteMessage(id)
        assertThat(messageEventTestListener.awaitMessageDeleteById(id)).isTrue()
	}

}
