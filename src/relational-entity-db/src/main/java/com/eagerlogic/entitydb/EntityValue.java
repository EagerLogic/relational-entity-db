/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.eagerlogic.entitydb;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 *
 * @author dipacs
 */
public class EntityValue {
    
    private final Map<String, Object> attributes = new HashMap<String, Object>();
    
    public void put(String name, Object value) {
        attributes.put(name, value);
    }
    
    public <T> T get(String name) {
        return (T) attributes.get(name);
    }
    
    public <T> T remove(String name) {
        return (T) attributes.remove(name);
    }
    
    public void clear() {
        attributes.clear();
    }
    
    public boolean hasName(String name) {
        return attributes.get(name) != null;
    }
    
    public Set<String> getNames() {
        return attributes.keySet();
    }
    
    public Set<Entry<String, Object>> entries() {
        return attributes.entrySet();
    }
    
    
}
