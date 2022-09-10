package be.twofold.crab.model;

import java.util.*;
import java.util.regex.*;

public interface Named {

    Pattern CAMEL_CASE = Pattern.compile("([a-z])([A-Z])");

    String getName();

    default String getPgName() {
        return CAMEL_CASE.matcher(getName())
            .replaceAll("$1_$2")
            .toLowerCase(Locale.ROOT);
    }

}
