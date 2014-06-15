package com.eagerlogic.entitydb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class represents a persistable object.
 *
 * @author dipacs
 */
public final class Entity implements Serializable {

    private long id = -1;
    private String kind;
    private byte[] value;
    private final Map<String, Object> attributes = new HashMap<String, Object>();

    Entity() {
    }

    /**
     * Creates a new Entity with the given kind.
     *
     * @param kind The kind of the entity.
     */
    public Entity(String kind) {
        this.kind = kind;
    }

    void setId(long id) {
        this.id = id;
    }

    /**
     * Returns the id of this entity. The id is filled automatically after
     * storing it in the database first, using the <code>DB.put(Entity)</code>
     * method.
     *
     * @return
     */
    public long getId() {
        return id;
    }

    /**
     * Returns the kind of this entity.
     *
     * @return The kind of this entity.
     */
    public String getKind() {
        return kind;
    }

    /**
     * Returns the value of this entity.
     *
     * @return The value of this entity.
     */
    byte[] getByteValue() {
        return value;
    }
    
//    public String getStringValue() {
//        if (value == null) {
//            return null;
//        }
//        
//        try {
//            return new String(value, "UTF-8");
//        } catch (UnsupportedEncodingException ex) {
//            throw new RuntimeException(ex);
//        }
//    }
    
    public EntityValue getValue() {
        return getObjectValue();
    }
    
    private <T> T getObjectValue() {
        if (value == null) {
            return null;
        }
        
        ByteArrayInputStream bais = null;
        ObjectInputStream ois = null;
        bais = new ByteArrayInputStream(value);
        try {
            ois = new ObjectInputStream(bais);
            return (T) ois.readObject();
        } catch (IOException | ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        } finally {
            if (bais != null) {
                try {
                    bais.close();
                } catch (IOException ex) {
                    Logger.getLogger(Entity.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (ois != null) {
                try {
                    ois.close();
                } catch (IOException ex) {
                    Logger.getLogger(Entity.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    /**
     * Sets the value of this entity.
     *
     * @param value The new value of this entity.
     */
    void setByteValue(byte[] value) {
        this.value = value;
    }
    
    public void setValue(EntityValue value) {
        setValue((Object) value);
    }
    
    private void setValue(Object obj) {
        if (obj == null) {
            this.value = null;
            return;
        }
        
        ObjectOutputStream oos = null;
        ByteArrayOutputStream baos = null;
        try {
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            oos.flush();
            this.value = baos.toByteArray();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            if (oos != null) {
                try {
                    oos.close();
                } catch (IOException ex) {
                    Logger.getLogger(Entity.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (baos != null) {
                try {
                    baos.close();
                } catch (IOException ex) {
                    Logger.getLogger(Entity.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }
    
//    public void setValue(String value) {
//        if (value == null) {
//            this.value = null;
//            return;
//        }
//        try {
//            this.value = value.getBytes("UTF-8");
//        } catch (UnsupportedEncodingException ex) {
//            throw new RuntimeException(ex);
//        }
//    }

    /**
     * Puts the given long value as an attribute associated with the given key.
     *
     * @param key The key of the given attribute.
     * @param value The value of the attribute.
     */
    public void putAttribute(String key, Long value) {
        attributes.put(key, value);
    }

    /**
     * Puts the given boolean value as an attribute associated with the given
     * key.
     *
     * @param key The key of the given attribute.
     * @param value The value of the attribute.
     */
    public void putAttribute(String key, Boolean value) {
        attributes.put(key, value);
    }

    /**
     * Puts the given String value as an attribute associated with the given
     * key.
     *
     * @param key The key of the given attribute.
     * @param value The value of the attribute, Can not be null.
     */
    public void putAttribute(String key, String value) {
        attributes.put(key, value);
    }

    /**
     * Returns the attribute associated with the given key as long.
     *
     * @param key The key of the attribute.
     *
     * @return The attribute associated with the given key.
     */
    public Long getLongAttribute(String key) {
        return ((Long) attributes.get(key));
    }

    /**
     * Returns the attribute associated with the given key as boolean.
     *
     * @param key The key of the attribute.
     *
     * @return The attribute associated with the given key.
     */
    public Boolean getBooleanAttribute(String key) {
        return ((Boolean) attributes.get(key));
    }

    /**
     * Returns the attribute associated with the given key as String.
     *
     * @param key The key of the attribute.
     *
     * @return The attribute associated with the given key.
     */
    public String getStringAttribute(String key) {
        return ((String) attributes.get(key));
    }

    /**
     * Returns the attribute associated with the given key as an Object.
     *
     * @param key The key of the attribute.
     *
     * @return The attribute associated with the given key.
     */
    public <T> T getAttribute(String key) {
        return (T) attributes.get(key);
    }

    /**
     * Indicates if the given key is null (not set).
     *
     * @param key The key of the attribute.
     *
     * @return True if the given attribute is null.
     */
    public boolean isAttributeNull(String key) {
        return attributes.get(key) == null;
    }

    /**
     * Returns the name of the attributes which are associated with this Entity.
     *
     * @return The name of the attributes which are associated with this Entity.
     */
    public Set<String> getAttributeNames() {
        return attributes.keySet();
    }
    
    Map<String, Object> getAttributes() {
        return attributes;
    }

}
