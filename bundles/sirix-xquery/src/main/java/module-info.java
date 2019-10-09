module sirix.xquery {
    requires auto.service;
    requires brackit;
    requires com.google.common;
    requires google.http.client;
    requires gson;
    requires java.xml;
    requires jsr305;
    requires slf4j.api;

    requires sirix.core;

    exports org.sirix.xquery;
    exports org.sirix.xquery.node;
}