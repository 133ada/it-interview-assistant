package interview.guide.modules.interview;

import interview.guide.modules.interview.service.InterviewHistoryService;
import interview.guide.modules.interview.service.InterviewPersistenceService;
import interview.guide.modules.interview.service.InterviewSessionService;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.annotation.Resource;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

class InterviewControllerTest {
    @Resource
    InterviewSessionService interviewSessionService;
    @Resource
    InterviewHistoryService interviewHistoryService;
    @Resource
    InterviewPersistenceService interviewPersistenceService;

    @Test
    void getReport() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class<?> clazz = Class.forName("interview.guide.modules.interview.InterviewController");
        Constructor<?> constructor = clazz.getDeclaredConstructor(InterviewSessionService.class, InterviewHistoryService.class, InterviewPersistenceService.class);
        InterviewController instance = (InterviewController) constructor.newInstance(interviewSessionService, interviewHistoryService, interviewPersistenceService);
        Method getReport = clazz.getMethod("getReport", String.class);
        Operation reportAnnotation = getReport.getAnnotation(Operation.class);
        System.out.println("getReport 方法 的 Operation 注解中，其 description 值为：" + reportAnnotation.description());
        System.out.println("getReport 方法 的 Operation 注解中，其 summary 值为：" + reportAnnotation.summary());
    }
}