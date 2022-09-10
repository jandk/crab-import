package be.twofold.crab.model;

import java.util.*;
import java.util.regex.*;

public interface Named {

    Pattern CAMEL_CASE = Pattern.compile("([a-z])([A-Z])");

    String name();

    default String getPgName() {
        return CAMEL_CASE.matcher(name())
            .replaceAll("$1_$2")
            .toLowerCase(Locale.ROOT);
    }

}
