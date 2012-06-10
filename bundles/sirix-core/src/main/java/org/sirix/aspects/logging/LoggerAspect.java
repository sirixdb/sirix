package org.sirix.aspects.logging;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Aspect
public class LoggerAspect {

  private static final ILoggerFactory FACTORY = LoggerFactory.getILoggerFactory();

  /**
   * 
   * 
   * @param pjp
   * @return
   * @throws Throwable
   */
  @Around("@annotation(org.sirix.aspects.logging.Logging)")
  public Object advice(ProceedingJoinPoint pjp) throws Throwable {
    final Signature sig = pjp.getSignature();
    final Logger logger = FACTORY.getLogger(sig.getDeclaringTypeName());
    logger.debug(new StringBuilder("Entering ").append(sig.getDeclaringTypeName()).toString());
    final Object returnVal = pjp.proceed();
    logger.debug(new StringBuilder("Exiting ").append(sig.getDeclaringTypeName()).toString());
    return returnVal;
  }

}
