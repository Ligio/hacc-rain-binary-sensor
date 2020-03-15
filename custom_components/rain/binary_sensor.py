"""Platform for sensor integration."""
from homeassistant.components.binary_sensor import BinarySensorDevice
from homeassistant.components.recorder import CONF_DB_URL, DEFAULT_URL, DEFAULT_DB_FILE, _LOGGER


def setup_platform(hass, config, add_entities, discovery_info=None):
    """Set up the SQL sensor platform."""
    db_url = config.get(CONF_DB_URL, None)
    if not db_url:
        db_url = DEFAULT_URL.format(hass_config_path=hass.config.path(DEFAULT_DB_FILE))

    import sqlalchemy
    from sqlalchemy.orm import sessionmaker, scoped_session

    try:
        engine = sqlalchemy.create_engine(db_url)
        sessionmaker = scoped_session(sessionmaker(bind=engine))

        # Run a dummy query just to test the db_url
        sess = sessionmaker()
        sess.execute("SELECT 1;")

    except sqlalchemy.exc.SQLAlchemyError as err:
        _LOGGER.error("Couldn't connect using %s DB_URL: %s", db_url, err)
        return

    add_entities([RainSensor("is raining", sessionmaker)])


class RainSensor(BinarySensorDevice):
    """Representation of a Sensor."""

    def __init__(self, name, sessmaker):
        self._name = name
        self._sessionmaker = sessmaker
        self._is_raining = False

    @property
    def name(self):
        """Return the name of the sensor."""
        return self._name

    @property
    def is_on(self):
        """Return True if the binary sensor is on."""
        self._update_state()
        _LOGGER.debug("Is raining? %s", str(self._is_raining))
        return self._is_raining

    def _update_state(self):
        min_value, max_value = self._get_data()
        if self._is_raining and max_value < 1:
            self._is_raining = False
        elif not self._is_raining and max_value > 0:
            self._is_raining = True

    def _get_data(self):
        """Retrieve sensor data from the query."""
        import sqlalchemy

        query = "SELECT state FROM \"states\" WHERE entity_id=\"sensor.rain_sensor\" AND created > Datetime('now', '-10 minutes') ORDER BY created DESC"
        try:
            sess = self._sessionmaker()
            result = sess.execute(query)

            if not result.returns_rows or result.rowcount == 0:
                _LOGGER.warning("%s returned no results", query)
                self._state = None
                return 0, 0

            data = []
            for res in result:
                for key, value in res.items():
                    try:
                        data.append(float(value))
                    except:
                        pass

            if len(data) > 0:
                return min(data), max(data)
            return 0, 0
        except sqlalchemy.exc.SQLAlchemyError as err:
            _LOGGER.error("Error executing query %s: %s", query, err)
            self._state = None
            return 0, 0
        finally:
            sess.close()
