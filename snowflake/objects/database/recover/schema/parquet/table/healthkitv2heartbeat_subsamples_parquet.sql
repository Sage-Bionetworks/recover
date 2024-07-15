CREATE OR ALTER TABLE HEALTHKITV2HEARTBEAT_SUBSAMPLES (
    "id" NUMBER(38,0),
    "index" NUMBER(38,0),
    "PrecededByGap" BOOLEAN,
    "TimeSinceSeriesStart" FLOAT,
    "ParticipantIdentifier" VARCHAR(16777216),
    "HealthKitHeartbeatSampleKey" VARCHAR(16777216),
    "ParticipantID" VARCHAR(16777216)
);
