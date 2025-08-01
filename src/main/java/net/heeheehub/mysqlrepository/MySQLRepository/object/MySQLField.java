package net.heeheehub.mysqlrepository.MySQLRepository.object;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface MySQLField {
    String value();
    boolean autoMapped() default false;
}
