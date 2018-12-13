package com.alooma.unlimited_kafka;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CapsuleTest {

    @Test
    void testToString() {

        assertEquals(Capsule.remoteCapsule("thisiskey").toString(), "Capsule{type=REMOTE, key='thisiskey', data=null}");
        assertEquals(Capsule.localCapsule("thisisdata").toString(), "Capsule{type=LOCAL, key='null', data=thisisdata}");

    }
}