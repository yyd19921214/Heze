package com.yudy.heze.config;

import com.yudy.heze.util.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class Config {

    private static final Logger LOGGER = LoggerFactory.getLogger(Config.class);

    protected final Properties properties;

    public Config(Properties props) {
        this.properties = props;
    }

    public Config(String filename) {
        File cfg = new File("./" + filename);
        properties = new Properties();
        FileInputStream fis = null;
        try {
            if (cfg.exists()) {
                fis = new FileInputStream(cfg);
                properties.load(fis);
            } else {
                LOGGER.error("config file is null");
                throw new RuntimeException("config file is null");
            }

        } catch (IOException ex) {
            LOGGER.error(ex.getMessage());
            throw new RuntimeException(ex);
        } finally {
            Closer.closeQuietly(fis);
        }
    }

    public Config(File cfg){
        properties = new Properties();
        FileInputStream fis = null;
        try {
            if (cfg.exists()) {
                fis = new FileInputStream(cfg);
                properties.load(fis);
            }else{
                LOGGER.error("config file is null");
                throw new RuntimeException("Config file is null");
            }
        } catch (IOException ex) {
            LOGGER.error(ex.getMessage());
            throw new RuntimeException(ex);
        } finally {
            Closer.closeQuietly(fis);
        }
    }

    public String getString(String name, String defaultValue) {
        return properties.containsKey(name) ? properties.getProperty(name) : defaultValue;
    }

    public String getString(String name) {
        if (properties.containsKey(name)) {
            return properties.getProperty(name);
        }
        throw new IllegalArgumentException("Missing required property '" + name + "'");
    }

    public int getInt(String name, int defaultValue) {
        return getIntInRange(name, defaultValue, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    public int getIntInRange(String name, int defaultValue, int min, int max) {
        int v = defaultValue;
        if (properties.containsKey(name))
            v = Integer.valueOf(properties.getProperty(name));
        if (v >= min && v <= max)
            return v;
        throw new IllegalArgumentException(name + " has value " + v + " which is not in the range");
    }

    public boolean getBoolean(String name, boolean defaultValue) {
        if (!properties.containsKey(name)) return defaultValue;
        return "true".equalsIgnoreCase(properties.getProperty(name));
    }

    protected int get(String name, int defaultValue) {
        return getInt(name, defaultValue);
    }

    protected String get(String name, String defaultValue) {
        return getString(name, defaultValue);
    }

    public Properties getProps(String name, Properties defaultProperties) {
        final String propString = properties.getProperty(name);
        if (propString == null) return defaultProperties;
        String[] propValues = propString.split(",");
        if (propValues.length < 1) {
            throw new IllegalArgumentException("Illegal format of specifying properties '" + propString + "'");
        }
        Properties properties = new Properties();
        for (int i = 0; i < propValues.length; i++) {
            String[] prop = propValues[i].split("=");
            if (prop.length != 2)
                throw new IllegalArgumentException("Illegal format of specifying properties '" + propValues[i] + "'");
            properties.put(prop[0], prop[1]);
        }
        return properties;
    }

    @SuppressWarnings("unchecked")
    public <E> E getObject(String className) {
        if (className == null) {
            return (E) null;
        }
        try {
            return (E) Class.forName(className).newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }


}
