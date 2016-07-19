package ro.teamnet.zth.api.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/*
* Vom folosi aceasta annotare pentru a marca entitatile si campurile acestora
* */
@Target(FIELD)
@Retention(RUNTIME)
public @interface Id {

    String name() default "id";
}
