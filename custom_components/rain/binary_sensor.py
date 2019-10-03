"""Platform for sensor integration."""
import decimal

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

    add_entities([RainSensor("is really raining", sessionmaker)])


class RainSensor(BinarySensorDevice):
    """Representation of a Sensor."""

    def __init__(self, name, sessmaker):
        self._name = name
        self._sessionmaker = sessmaker
        self._is_raining = False

    @property
    def is_on(self):
        """Return True if the binary sensor is on."""
        self._update_state()
        return self._is_raining

    def _update_state(self):
        min_value, max_value = self._get_data()
        if self._is_raining:
            if max_value < 20:
                self._is_raining = False
                return

            if max_value < 40 and max_value - min_value < 5:
                self._is_raining = False
                return
        else:
            if max_value > 40:
                self._is_raining = True
                return

            if max_value > 20 and min_value < 10:
                self._is_raining = True
                return


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
                return

            data = []
            for res in result:
                _LOGGER.debug("result = %s", res.items())
                for key, value in res.items():
                    if isinstance(value, decimal.Decimal):
                        data.append(float(value))

            return min(data), max(data)
        except sqlalchemy.exc.SQLAlchemyError as err:
            _LOGGER.error("Error executing query %s: %s", query, err)
            return
        finally:
            sess.close()

