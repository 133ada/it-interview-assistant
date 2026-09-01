package interview.guide.infrastructure.mapper;

import interview.guide.modules.knowledgebase.model.RagChatDTO;
import interview.guide.modules.knowledgebase.model.RagChatMessageEntity;
import interview.guide.modules.knowledgebase.model.RagChatSessionEntity;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.processing.Generated;
import org.springframework.stereotype.Component;

@Generated(
    value = "org.mapstruct.ap.MappingProcessor",
    date = "2026-08-12T17:36:27+0800",
    comments = "version: 1.6.3, compiler: IncrementalProcessingEnvironment from gradle-language-java-8.14.jar, environment: Java 21.0.1 (Oracle Corporation)"
)
@Component
public class RagChatMapperImpl implements RagChatMapper {

    @Override
    public RagChatDTO.SessionDTO toSessionDTO(RagChatSessionEntity session) {
        if ( session == null ) {
            return null;
        }

        List<Long> knowledgeBaseIds = null;
        Long id = null;
        String title = null;
        LocalDateTime createdAt = null;

        knowledgeBaseIds = extractKnowledgeBaseIds( session );
        id = session.getId();
        title = session.getTitle();
        createdAt = session.getCreatedAt();

        RagChatDTO.SessionDTO sessionDTO = new RagChatDTO.SessionDTO( id, title, knowledgeBaseIds, createdAt );

        return sessionDTO;
    }

    @Override
    public RagChatDTO.MessageDTO toMessageDTO(RagChatMessageEntity message) {
        if ( message == null ) {
            return null;
        }

        String type = null;
        Long id = null;
        String content = null;
        LocalDateTime createdAt = null;

        type = getTypeString( message );
        id = message.getId();
        content = message.getContent();
        createdAt = message.getCreatedAt();

        RagChatDTO.MessageDTO messageDTO = new RagChatDTO.MessageDTO( id, type, content, createdAt );

        return messageDTO;
    }

    @Override
    public List<RagChatDTO.MessageDTO> toMessageDTOList(List<RagChatMessageEntity> messages) {
        if ( messages == null ) {
            return null;
        }

        List<RagChatDTO.MessageDTO> list = new ArrayList<RagChatDTO.MessageDTO>( messages.size() );
        for ( RagChatMessageEntity ragChatMessageEntity : messages ) {
            list.add( toMessageDTO( ragChatMessageEntity ) );
        }

        return list;
    }

    @Override
    public RagChatDTO.SessionListItemDTO toSessionListItemDTO(RagChatSessionEntity session) {
        if ( session == null ) {
            return null;
        }

        List<String> knowledgeBaseNames = null;
        Boolean isPinned = null;
        Long id = null;
        String title = null;
        Integer messageCount = null;
        LocalDateTime updatedAt = null;

        knowledgeBaseNames = extractKnowledgeBaseNames( session.getKnowledgeBases() );
        isPinned = getIsPinnedWithDefault( session );
        id = session.getId();
        title = session.getTitle();
        messageCount = session.getMessageCount();
        updatedAt = session.getUpdatedAt();

        RagChatDTO.SessionListItemDTO sessionListItemDTO = new RagChatDTO.SessionListItemDTO( id, title, messageCount, knowledgeBaseNames, updatedAt, isPinned );

        return sessionListItemDTO;
    }
}
