package com.alooma.unlimited_kafka;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CapsuleTest {

    @Test
    void testToString() {

        assertEquals(Capsule.remoteCapsule("thisiskey").toString(), "Capsule{type=REMOTE, key='thisiskey', data=null}");
        assertEquals(Capsule.localCapsule("thisisdata").toString(), "Capsule{type=LOCAL, key='null', data=thisisdata}");
    }

    @Test
    void testHashCode() {
        assertEquals(Capsule.localCapsule("a").hashCode(), Capsule.localCapsule("a").hashCode());
        assertNotEquals(Capsule.localCapsule("a").hashCode(), Capsule.localCapsule("b").hashCode());

        assertEquals(Capsule.remoteCapsule("a").hashCode(), Capsule.remoteCapsule("a").hashCode());
        assertNotEquals(Capsule.remoteCapsule("a").hashCode(), Capsule.remoteCapsule("b").hashCode());
    }

    @Test
    void testEquals() {
        Capsule<String> capsule = Capsule.localCapsule("a");
        assertEquals(capsule, capsule);
        assertNotNull(capsule);
        assertNotEquals(capsule, "notcapsule");

    }
}