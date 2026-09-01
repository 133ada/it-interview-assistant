package interview.guide.infrastructure.mapper;

import interview.guide.modules.knowledgebase.model.KnowledgeBaseEntity;
import interview.guide.modules.knowledgebase.model.KnowledgeBaseListItemDTO;
import interview.guide.modules.knowledgebase.model.VectorStatus;
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
public class KnowledgeBaseMapperImpl implements KnowledgeBaseMapper {

    @Override
    public KnowledgeBaseListItemDTO toListItemDTO(KnowledgeBaseEntity entity) {
        if ( entity == null ) {
            return null;
        }

        Long id = null;
        String name = null;
        String category = null;
        String originalFilename = null;
        Long fileSize = null;
        String contentType = null;
        LocalDateTime uploadedAt = null;
        LocalDateTime lastAccessedAt = null;
        Integer accessCount = null;
        Integer questionCount = null;
        VectorStatus vectorStatus = null;
        String vectorError = null;
        Integer chunkCount = null;

        id = entity.getId();
        name = entity.getName();
        category = entity.getCategory();
        originalFilename = entity.getOriginalFilename();
        fileSize = entity.getFileSize();
        contentType = entity.getContentType();
        uploadedAt = entity.getUploadedAt();
        lastAccessedAt = entity.getLastAccessedAt();
        accessCount = entity.getAccessCount();
        questionCount = entity.getQuestionCount();
        vectorStatus = entity.getVectorStatus();
        vectorError = entity.getVectorError();
        chunkCount = entity.getChunkCount();

        KnowledgeBaseListItemDTO knowledgeBaseListItemDTO = new KnowledgeBaseListItemDTO( id, name, category, originalFilename, fileSize, contentType, uploadedAt, lastAccessedAt, accessCount, questionCount, vectorStatus, vectorError, chunkCount );

        return knowledgeBaseListItemDTO;
    }

    @Override
    public List<KnowledgeBaseListItemDTO> toListItemDTOList(List<KnowledgeBaseEntity> entities) {
        if ( entities == null ) {
            return null;
        }

        List<KnowledgeBaseListItemDTO> list = new ArrayList<KnowledgeBaseListItemDTO>( entities.size() );
        for ( KnowledgeBaseEntity knowledgeBaseEntity : entities ) {
            list.add( toListItemDTO( knowledgeBaseEntity ) );
        }

        return list;
    }
}
