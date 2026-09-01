package interview.guide.infrastructure.mapper;

import interview.guide.common.model.AsyncTaskStatus;
import interview.guide.modules.interview.model.InterviewHistoryItemDTO;
import interview.guide.modules.interview.model.ResumeAnalysisResponse;
import interview.guide.modules.resume.model.ResumeAnalysisEntity;
import interview.guide.modules.resume.model.ResumeDetailDTO;
import interview.guide.modules.resume.model.ResumeEntity;
import interview.guide.modules.resume.model.ResumeListItemDTO;
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
public class ResumeMapperImpl implements ResumeMapper {

    @Override
    public ResumeAnalysisResponse.ScoreDetail toScoreDetail(ResumeAnalysisEntity entity) {
        if ( entity == null ) {
            return null;
        }

        int contentScore = 0;
        int structureScore = 0;
        int skillMatchScore = 0;
        int expressionScore = 0;
        int projectScore = 0;

        contentScore = nullToZero( entity.getContentScore() );
        structureScore = nullToZero( entity.getStructureScore() );
        skillMatchScore = nullToZero( entity.getSkillMatchScore() );
        expressionScore = nullToZero( entity.getExpressionScore() );
        projectScore = nullToZero( entity.getProjectScore() );

        ResumeAnalysisResponse.ScoreDetail scoreDetail = new ResumeAnalysisResponse.ScoreDetail( contentScore, structureScore, skillMatchScore, expressionScore, projectScore );

        return scoreDetail;
    }

    @Override
    public ResumeListItemDTO toListItemDTOBasic(ResumeEntity entity) {
        if ( entity == null ) {
            return null;
        }

        String filename = null;
        Long id = null;
        Long fileSize = null;
        LocalDateTime uploadedAt = null;
        Integer accessCount = null;
        AsyncTaskStatus analyzeStatus = null;
        String analyzeError = null;

        filename = entity.getOriginalFilename();
        id = entity.getId();
        fileSize = entity.getFileSize();
        uploadedAt = entity.getUploadedAt();
        accessCount = entity.getAccessCount();
        analyzeStatus = entity.getAnalyzeStatus();
        analyzeError = entity.getAnalyzeError();

        Integer latestScore = null;
        LocalDateTime lastAnalyzedAt = null;
        Integer interviewCount = null;

        ResumeListItemDTO resumeListItemDTO = new ResumeListItemDTO( id, filename, fileSize, uploadedAt, accessCount, latestScore, lastAnalyzedAt, interviewCount, analyzeStatus, analyzeError );

        return resumeListItemDTO;
    }

    @Override
    public ResumeDetailDTO toDetailDTOBasic(ResumeEntity entity) {
        if ( entity == null ) {
            return null;
        }

        String filename = null;
        Long id = null;
        Long fileSize = null;
        String contentType = null;
        String storageUrl = null;
        LocalDateTime uploadedAt = null;
        Integer accessCount = null;
        String resumeText = null;
        AsyncTaskStatus analyzeStatus = null;
        String analyzeError = null;

        filename = entity.getOriginalFilename();
        id = entity.getId();
        fileSize = entity.getFileSize();
        contentType = entity.getContentType();
        storageUrl = entity.getStorageUrl();
        uploadedAt = entity.getUploadedAt();
        accessCount = entity.getAccessCount();
        resumeText = entity.getResumeText();
        analyzeStatus = entity.getAnalyzeStatus();
        analyzeError = entity.getAnalyzeError();

        List<ResumeDetailDTO.AnalysisHistoryDTO> analyses = null;
        List<InterviewHistoryItemDTO> interviews = null;

        ResumeDetailDTO resumeDetailDTO = new ResumeDetailDTO( id, filename, fileSize, contentType, storageUrl, uploadedAt, accessCount, resumeText, analyzeStatus, analyzeError, analyses, interviews );

        return resumeDetailDTO;
    }

    @Override
    public ResumeDetailDTO.AnalysisHistoryDTO toAnalysisHistoryDTO(ResumeAnalysisEntity entity, List<String> strengths, List<Object> suggestions) {
        if ( entity == null && strengths == null && suggestions == null ) {
            return null;
        }

        Long id = null;
        Integer overallScore = null;
        Integer contentScore = null;
        Integer structureScore = null;
        Integer skillMatchScore = null;
        Integer expressionScore = null;
        Integer projectScore = null;
        String summary = null;
        LocalDateTime analyzedAt = null;
        if ( entity != null ) {
            id = entity.getId();
            overallScore = entity.getOverallScore();
            contentScore = entity.getContentScore();
            structureScore = entity.getStructureScore();
            skillMatchScore = entity.getSkillMatchScore();
            expressionScore = entity.getExpressionScore();
            projectScore = entity.getProjectScore();
            summary = entity.getSummary();
            analyzedAt = entity.getAnalyzedAt();
        }
        List<String> strengths1 = null;
        List<String> list = strengths;
        if ( list != null ) {
            strengths1 = new ArrayList<String>( list );
        }
        List<Object> suggestions1 = null;
        List<Object> list1 = suggestions;
        if ( list1 != null ) {
            suggestions1 = new ArrayList<Object>( list1 );
        }

        ResumeDetailDTO.AnalysisHistoryDTO analysisHistoryDTO = new ResumeDetailDTO.AnalysisHistoryDTO( id, overallScore, contentScore, structureScore, skillMatchScore, expressionScore, projectScore, summary, analyzedAt, strengths1, suggestions1 );

        return analysisHistoryDTO;
    }

    @Override
    public ResumeAnalysisEntity toAnalysisEntity(ResumeAnalysisResponse response) {
        if ( response == null ) {
            return null;
        }

        ResumeAnalysisEntity resumeAnalysisEntity = new ResumeAnalysisEntity();

        resumeAnalysisEntity.setContentScore( responseScoreDetailContentScore( response ) );
        resumeAnalysisEntity.setStructureScore( responseScoreDetailStructureScore( response ) );
        resumeAnalysisEntity.setSkillMatchScore( responseScoreDetailSkillMatchScore( response ) );
        resumeAnalysisEntity.setExpressionScore( responseScoreDetailExpressionScore( response ) );
        resumeAnalysisEntity.setProjectScore( responseScoreDetailProjectScore( response ) );
        resumeAnalysisEntity.setOverallScore( response.overallScore() );
        resumeAnalysisEntity.setSummary( response.summary() );

        return resumeAnalysisEntity;
    }

    @Override
    public void updateAnalysisEntity(ResumeAnalysisResponse response, ResumeAnalysisEntity entity) {
        if ( response == null ) {
            return;
        }

        entity.setContentScore( responseScoreDetailContentScore( response ) );
        entity.setStructureScore( responseScoreDetailStructureScore( response ) );
        entity.setSkillMatchScore( responseScoreDetailSkillMatchScore( response ) );
        entity.setExpressionScore( responseScoreDetailExpressionScore( response ) );
        entity.setProjectScore( responseScoreDetailProjectScore( response ) );
        entity.setOverallScore( response.overallScore() );
        entity.setSummary( response.summary() );
    }

    private Integer responseScoreDetailContentScore(ResumeAnalysisResponse resumeAnalysisResponse) {
        ResumeAnalysisResponse.ScoreDetail scoreDetail = resumeAnalysisResponse.scoreDetail();
        if ( scoreDetail == null ) {
            return null;
        }
        return scoreDetail.contentScore();
    }

    private Integer responseScoreDetailStructureScore(ResumeAnalysisResponse resumeAnalysisResponse) {
        ResumeAnalysisResponse.ScoreDetail scoreDetail = resumeAnalysisResponse.scoreDetail();
        if ( scoreDetail == null ) {
            return null;
        }
        return scoreDetail.structureScore();
    }

    private Integer responseScoreDetailSkillMatchScore(ResumeAnalysisResponse resumeAnalysisResponse) {
        ResumeAnalysisResponse.ScoreDetail scoreDetail = resumeAnalysisResponse.scoreDetail();
        if ( scoreDetail == null ) {
            return null;
        }
        return scoreDetail.skillMatchScore();
    }

    private Integer responseScoreDetailExpressionScore(ResumeAnalysisResponse resumeAnalysisResponse) {
        ResumeAnalysisResponse.ScoreDetail scoreDetail = resumeAnalysisResponse.scoreDetail();
        if ( scoreDetail == null ) {
            return null;
        }
        return scoreDetail.expressionScore();
    }

    private Integer responseScoreDetailProjectScore(ResumeAnalysisResponse resumeAnalysisResponse) {
        ResumeAnalysisResponse.ScoreDetail scoreDetail = resumeAnalysisResponse.scoreDetail();
        if ( scoreDetail == null ) {
            return null;
        }
        return scoreDetail.projectScore();
    }
}
