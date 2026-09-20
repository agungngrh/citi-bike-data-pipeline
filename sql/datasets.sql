CREATE SCHEMA IF NOT EXISTS `jcdeah-009.citibike_raw`
OPTIONS (
  location = 'asia-southeast2',
);

CREATE SCHEMA IF NOT EXISTS `jcdeah-009.citibike_staging`
OPTIONS (
  location = 'asia-southeast2',
);

CREATE SCHEMA IF NOT EXISTS `jcdeah-009.citibike_mart`
OPTIONS (
  location = 'asia-southeast2',
);

CREATE SCHEMA IF NOT EXISTS `jcdeah-009.citibike_ops`
OPTIONS (
  location = 'asia-southeast2',
);

CREATE SCHEMA IF NOT EXISTS `jcdeah-009.citibike_intermediate`
OPTIONS (
  location = 'asia-southeast2',
);