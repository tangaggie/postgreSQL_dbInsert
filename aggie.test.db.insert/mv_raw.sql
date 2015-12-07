CREATE TABLE mv_raw
(
  measurement_id bigint NOT NULL,
  valid_ts timestamp with time zone NOT NULL,
  mvalue numeric(19,4) NOT NULL,
  CONSTRAINT pk_mv_raw PRIMARY KEY (measurement_id, valid_ts)
)
WITH (OIDS=FALSE)