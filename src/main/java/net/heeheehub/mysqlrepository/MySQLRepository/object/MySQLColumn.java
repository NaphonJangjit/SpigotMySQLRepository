package net.heeheehub.mysqlrepository.MySQLRepository.object;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface MySQLColumn {
    String value();
    boolean isNotNull() default false;
}
