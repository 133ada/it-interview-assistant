package interview.guide.infrastructure.mapper;

import interview.guide.modules.interview.model.InterviewAnswerEntity;
import interview.guide.modules.interview.model.InterviewDetailDTO;
import interview.guide.modules.interview.model.InterviewReportDTO;
import interview.guide.modules.interview.model.InterviewSessionEntity;
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
public class InterviewMapperImpl implements InterviewMapper {

    @Override
    public InterviewReportDTO.QuestionEvaluation toQuestionEvaluation(InterviewAnswerEntity entity) {
        if ( entity == null ) {
            return null;
        }

        int questionIndex = 0;
        String question = null;
        String category = null;
        String userAnswer = null;
        int score = 0;
        String feedback = null;

        questionIndex = nullIndexToZero( entity.getQuestionIndex() );
        question = entity.getQuestion();
        category = entity.getCategory();
        userAnswer = entity.getUserAnswer();
        score = nullScoreToZero( entity.getScore() );
        feedback = entity.getFeedback();

        InterviewReportDTO.QuestionEvaluation questionEvaluation = new InterviewReportDTO.QuestionEvaluation( questionIndex, question, category, userAnswer, score, feedback );

        return questionEvaluation;
    }

    @Override
    public List<InterviewReportDTO.QuestionEvaluation> toQuestionEvaluations(List<InterviewAnswerEntity> entities) {
        if ( entities == null ) {
            return null;
        }

        List<InterviewReportDTO.QuestionEvaluation> list = new ArrayList<InterviewReportDTO.QuestionEvaluation>( entities.size() );
        for ( InterviewAnswerEntity interviewAnswerEntity : entities ) {
            list.add( toQuestionEvaluation( interviewAnswerEntity ) );
        }

        return list;
    }

    @Override
    public InterviewDetailDTO.AnswerDetailDTO toAnswerDetailDTO(InterviewAnswerEntity entity, List<String> keyPoints) {
        if ( entity == null && keyPoints == null ) {
            return null;
        }

        Integer questionIndex = null;
        String question = null;
        String category = null;
        String userAnswer = null;
        Integer score = null;
        String feedback = null;
        String referenceAnswer = null;
        LocalDateTime answeredAt = null;
        if ( entity != null ) {
            questionIndex = entity.getQuestionIndex();
            question = entity.getQuestion();
            category = entity.getCategory();
            userAnswer = entity.getUserAnswer();
            score = entity.getScore();
            feedback = entity.getFeedback();
            referenceAnswer = entity.getReferenceAnswer();
            answeredAt = entity.getAnsweredAt();
        }
        List<String> keyPoints1 = null;
        List<String> list = keyPoints;
        if ( list != null ) {
            keyPoints1 = new ArrayList<String>( list );
        }

        InterviewDetailDTO.AnswerDetailDTO answerDetailDTO = new InterviewDetailDTO.AnswerDetailDTO( questionIndex, question, category, userAnswer, score, feedback, referenceAnswer, keyPoints1, answeredAt );

        return answerDetailDTO;
    }

    @Override
    public InterviewDetailDTO toDetailDTO(InterviewSessionEntity session, List<Object> questions, List<String> strengths, List<String> improvements, List<Object> referenceAnswers, List<InterviewDetailDTO.AnswerDetailDTO> answers) {
        if ( session == null && questions == null && strengths == null && improvements == null && referenceAnswers == null && answers == null ) {
            return null;
        }

        String evaluateError = null;
        Long id = null;
        String sessionId = null;
        Integer totalQuestions = null;
        Integer overallScore = null;
        String overallFeedback = null;
        LocalDateTime createdAt = null;
        LocalDateTime completedAt = null;
        if ( session != null ) {
            evaluateError = session.getEvaluateError();
            id = session.getId();
            sessionId = session.getSessionId();
            totalQuestions = session.getTotalQuestions();
            overallScore = session.getOverallScore();
            overallFeedback = session.getOverallFeedback();
            createdAt = session.getCreatedAt();
            completedAt = session.getCompletedAt();
        }
        List<Object> questions1 = null;
        List<Object> list = questions;
        if ( list != null ) {
            questions1 = new ArrayList<Object>( list );
        }
        List<String> strengths1 = null;
        List<String> list1 = strengths;
        if ( list1 != null ) {
            strengths1 = new ArrayList<String>( list1 );
        }
        List<String> improvements1 = null;
        List<String> list2 = improvements;
        if ( list2 != null ) {
            improvements1 = new ArrayList<String>( list2 );
        }
        List<Object> referenceAnswers1 = null;
        List<Object> list3 = referenceAnswers;
        if ( list3 != null ) {
            referenceAnswers1 = new ArrayList<Object>( list3 );
        }
        List<InterviewDetailDTO.AnswerDetailDTO> answers1 = null;
        List<InterviewDetailDTO.AnswerDetailDTO> list4 = answers;
        if ( list4 != null ) {
            answers1 = new ArrayList<InterviewDetailDTO.AnswerDetailDTO>( list4 );
        }

        String status = session.getStatus().toString();
        String evaluateStatus = session.getEvaluateStatus() != null ? session.getEvaluateStatus().name() : null;

        InterviewDetailDTO interviewDetailDTO = new InterviewDetailDTO( id, sessionId, totalQuestions, status, evaluateStatus, evaluateError, overallScore, overallFeedback, createdAt, completedAt, questions1, strengths1, improvements1, referenceAnswers1, answers1 );

        return interviewDetailDTO;
    }

    @Override
    public void updateSessionFromReport(InterviewReportDTO report, InterviewSessionEntity session) {
        if ( report == null ) {
            return;
        }

        session.setOverallScore( report.overallScore() );
        session.setOverallFeedback( report.overallFeedback() );
    }
}
