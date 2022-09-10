package be.twofold.crab;

import be.twofold.crab.model.*;

import java.sql.*;
import java.util.*;

final class Tables {

    private Tables() {
        throw new UnsupportedOperationException();
    }

    static final Column Id = Column.fixed("ID", Types.INTEGER);

    static final List<Column> Metadata = List.of(
        Column.fixed("BEGINDATUM", Types.DATE),
        Column.fixed("EINDDATUM", Types.DATE).withNulls(),
        Column.variable("BEGINTIJD", Types.VARCHAR, 15),
        Column.fixed("BEGINBEW", Types.SMALLINT),
        Column.fixed("BEGINORG", Types.SMALLINT)
    );

    static final Table Gemeente = new Table("GEM", List.of(
        Column.fixed("NISGEMCODE", Types.INTEGER),
        Column.variable("TAALCODE", Types.VARCHAR, 2),
        Column.variable("TAALCODE2", Types.VARCHAR, 2).withNulls()
    ));

    static final Table GemeenteNaam = new Table("GEMNM", List.of(
        Column.fixed("GEMID", Types.INTEGER),
        Column.variable("GEMNM", Types.VARCHAR, 40),
        Column.variable("TAALCODE", Types.VARCHAR, 2)
    ));

    static final Table HuisNummer = new Table("HUISNR", List.of(
        Column.fixed("STRAATNMID", Types.INTEGER),
        Column.variable("HUISNR", Types.VARCHAR, 11),
        Column.variable("HUISNRID0", Types.VARCHAR, 37)
    ));

    static final Table KadGemeente = new Table("KADGEM", List.of(
        Column.fixed("KADGEMCODE", Types.INTEGER)
    ));

    static final Table KadGemeenteGemeente = new Table("KADGGEM", List.of(
        Column.fixed("KADGEMID", Types.INTEGER),
        Column.fixed("GEMID", Types.INTEGER)
    ));

    static final Table KadGemeenteNaam = new Table("KADGNM", List.of(
        Column.fixed("KADGEMID", Types.INTEGER),
        Column.variable("KADGEMNM", Types.VARCHAR, 80),
        Column.variable("TAALCODE", Types.VARCHAR, 2)
    ));

    static final Table PostKantonCode = new Table("PKANCODE", List.of(
        Column.fixed("HUISNRID", Types.INTEGER),
        Column.fixed("PKANCODE", Types.SMALLINT)
    ));

    static final Table PostKanton = new Table("POSTKAN", List.of(
        Column.fixed("PKANCODE", Types.SMALLINT)
    ));

    static final Table PostKantonNaam = new Table("POSTKNM", List.of(
        Column.fixed("POSTKANID", Types.INTEGER),
        Column.variable("POSTKANNM", Types.VARCHAR, 254),
        Column.variable("TAALCODE", Types.VARCHAR, 2)
    ));

    static final Table SubStraatStraatNaam = new Table("SSTRSTRN", List.of(
        Column.fixed("SUBSTRID", Types.INTEGER),
        Column.fixed("STRAATNMID", Types.INTEGER)
    ));

    static final Table StraatNaam = new Table("STRAATNM", List.of(
        Column.fixed("NISGEMCODE", Types.INTEGER),
        Column.variable("STRAATNM", Types.VARCHAR, 80),
        Column.variable("TAALCODE", Types.VARCHAR, 2),
        Column.variable("STRAATNM2", Types.VARCHAR, 80).withNulls(),
        Column.variable("TAALCODE2", Types.VARCHAR, 2).withNulls(),
        Column.variable("STRAATNM0", Types.VARCHAR, 80)
    ));

    static final Table StraatKant = new Table("STRKANT", List.of(
        Column.fixed("STRAATNMID", Types.INTEGER),
        Column.fixed("WEGOBJID", Types.INTEGER),
        Column.fixed("KANT", Types.SMALLINT),
        Column.fixed("BEGINPOS", Types.FLOAT),
        Column.fixed("EINDPOS", Types.FLOAT).withNulls(),
        Column.fixed("PARITEIT", Types.SMALLINT).withNulls(),
        Column.variable("EERSTEHNR", Types.VARCHAR, 20).withNulls(),
        Column.variable("LAATSTEHNR", Types.VARCHAR, 20).withNulls()
    ));

    static final Table SubAdres = new Table("SUBADRES", List.of(
        Column.fixed("HUISNRID", Types.INTEGER),
        Column.variable("SUBADR", Types.VARCHAR, 35).withNulls(),
        Column.fixed("AARD", Types.SMALLINT)
    ));

    static final Table SubKanton = new Table("SUBKAN", List.of(
        Column.fixed("POSTKANID", Types.INTEGER),
        Column.fixed("SUBKANNR", Types.SMALLINT)
    ));

    static final Table SubKantonGemeente = new Table("SUBKGEM", List.of(
        Column.fixed("SUBKANID", Types.INTEGER),
        Column.fixed("GEMID", Types.INTEGER)
    ));

    static final Table SubStraat = new Table("SUBSTR", List.of(
        Column.variable("STRAATCODE", Types.VARCHAR, 4),
        Column.fixed("SUBKANCODE", Types.SMALLINT)
    ));

    static final Table SubStraatNaam = new Table("SUBSTRNM", List.of(
        Column.fixed("SUBSTRID", Types.INTEGER),
        Column.variable("SUBSTRNM", Types.VARCHAR, 80),
        Column.variable("TAALCODE", Types.VARCHAR, 2)
    ));

    static final Table TerreinObject = new Table("TERROBJ", List.of(
        Column.variable("OBJID", Types.VARCHAR, 21),
        Column.fixed("AARD", Types.SMALLINT),
        Column.fixed("X", Types.FLOAT).withNulls(),
        Column.fixed("Y", Types.FLOAT).withNulls(),
        Column.fixed("KADGEMCODE", Types.INTEGER).withNulls()
    ));

    static final Table TerreinObjectHuisNummer = new Table("TOBJHNR", List.of(
        Column.fixed("TERROBJID", Types.INTEGER),
        Column.fixed("HUISNRID", Types.INTEGER)
    ));

    static final Table WegObject = new Table("WEGOBJ", List.of(
        Column.variable("OBJID", Types.VARCHAR, 21),
        Column.fixed("AARD", Types.SMALLINT)
    ));

    static final Map<String, Table> Tables = Map.ofEntries(
        Map.entry("gem", Gemeente),
        Map.entry("gemnm", GemeenteNaam),
        Map.entry("huisnr", HuisNummer),
        Map.entry("kadgem", KadGemeente),
        Map.entry("kadggem", KadGemeenteGemeente),
        Map.entry("kadgnm", KadGemeenteNaam),
        Map.entry("pkancode", PostKantonCode),
        Map.entry("postkan", PostKanton),
        Map.entry("postknm", PostKantonNaam),
        Map.entry("sstrstrn", SubStraatStraatNaam),
        Map.entry("straatnm", StraatNaam),
        Map.entry("strkant", StraatKant),
        Map.entry("subadres", SubAdres),
        Map.entry("subkan", SubKanton),
        Map.entry("subkgem", SubKantonGemeente),
        Map.entry("substr", SubStraat),
        Map.entry("substrnm", SubStraatNaam),
        Map.entry("terrobj", TerreinObject),
        Map.entry("tobjhnr", TerreinObjectHuisNummer),
        Map.entry("wegobj", WegObject)
    );

    static List<Column> getColumnsFor(String tableName, boolean metadata) {
        List<Column> columns = new ArrayList<>();
        columns.add(Id);
        columns.addAll(Tables.get(tableName).getColumns());
        if (metadata) {
            columns.addAll(Metadata);
        }
        return List.copyOf(columns);
    }

}
