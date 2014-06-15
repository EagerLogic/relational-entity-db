/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.eagerlogic.entitydb;

import java.io.File;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.rowset.serial.SerialBlob;

/**
 *
 * @author dipacs
 */
class RelationalDB {

    public static RelationalDB openOrCreateDatabase(String url) throws DatabaseException {
        if (isDatabaseExists(url)) {
            return new RelationalDB(url, false);
        } 
        return new RelationalDB(url, true);
    }

    public static RelationalDB openDatabase(String url) throws DatabaseException {
        return new RelationalDB(url, false);
    }

    public static boolean isDatabaseExists(String url) {
        return new File(url).exists();
    }

    private final String url;
    private final Connection conn;

    private RelationalDB(String url, boolean create) throws DatabaseException {
        this.url = url;

        try {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        } catch (ClassNotFoundException ex) {
            throw new DatabaseException("Can't find driver: org.apache.derby.jdbc.EmbeddedDriver");
        }

        boolean exists = isDatabaseExists(url);
        if (!exists) {
            if (!create) {
                throw new DatabaseException("Database does not exists: " + url);
            }
        }

        String connStr = "jdbc:derby:" + url + ";create=" + create;
        try {
            conn = DriverManager.getConnection(connStr);
        } catch (SQLException ex) {
            throw new DatabaseException("Can't open database. Reason: " + ex.getMessage(), ex);
        }

        if (!exists) {
            init();
        }
    }

    private void init() throws DatabaseException {
        String entitySql = "CREATE TABLE Entity ("
                + "id BIGINT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),"
                + "kind VARCHAR(4096) NOT NULL,"
                + "value BLOB)";

        String attributeQuery = "CREATE TABLE Attribute ("
                + "id BIGINT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),"
                + "entityId BIGINT NOT NULL,"
                + "entityKind VARCHAR(4096) NOT NULL,"
                + "name VARCHAR(128) NOT NULL,"
                + "type INT NOT NULL,"
                + "value VARCHAR(32000) NOT NULL)";

        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(entitySql);
            stmt.execute(attributeQuery);
        } catch (SQLException ex) {
            Logger.getLogger(RelationalDB.class.getName()).log(Level.SEVERE, null, ex);
            throw new DatabaseException("Initializing database failed.", ex);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    Logger.getLogger(RelationalDB.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    public void close() {
        try {
            conn.close();
            DriverManager.getConnection("jdbc:derby:;shutdown=true");
        } catch (SQLException ex) {
            Logger.getLogger(RelationalDB.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public Entity get(long id) {
        assertClosed();

        Entity res = null;

        String sql = "SELECT * FROM Entity WHERE id=" + id;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            if (rs.next()) {
                res = new Entity(rs.getString("kind"));
                res.setId(rs.getLong("id"));
                Blob value = rs.getBlob("value");
                res.setByteValue(value == null ? null : value.getBytes(1, (int) value.length()));

                Collection<AttributeDTO> attributes = getAttributes(id).values();
                for (AttributeDTO attr : attributes) {
                    if (attr.getType() == AttributeDTO.EType.BOOLEAN) {
                        res.putAttribute(attr.getName(), Boolean.valueOf(attr.getValue()));
                    } else if (attr.getType() == AttributeDTO.EType.LONG) {
                        res.putAttribute(attr.getName(), Long.parseLong(attr.getValue()));
                    } else if (attr.getType() == AttributeDTO.EType.STRING) {
                        res.putAttribute(attr.getName(), attr.getValue());
                    } else {
                        throw new RuntimeException("Unknown attribute type: " + attr.getType());
                    }
                }
            }
        } catch (SQLException | DatabaseException ex) {
            throw new RuntimeException(ex);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    Logger.getLogger(RelationalDB.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    Logger.getLogger(RelationalDB.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }

        return res;
    }

    public void put(Entity entity) {
        if (entity.getId() > -1) {
            update(entity);
        } else {
            putNew(entity);
        }
    }

    private void putNew(Entity entity) {
        String sql = "INSERT INTO Entity (kind, value) VALUES (?, ?)";

        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, entity.getKind());
            if (entity.getByteValue() != null) {
                ps.setBlob(2, new SerialBlob(entity.getByteValue()));
            } else {
                ps.setNull(2, Types.BLOB);
            }
            ps.execute();
            entity.setId(getLastInsertedId(ps));

            for (Map.Entry<String, Object> entry : entity.getAttributes().entrySet()) {
                putAttribute(entity.getId(), entity.getKind(), entry.getKey(), entry.getValue());
            }

        } catch (SQLException | DatabaseException ex) {
            throw new RuntimeException(ex);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException ex) {
                    Logger.getLogger(RelationalDB.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    private void putAttribute(long entityId, String entityKind, String name, Object value) {
        AttributeDTO.EType type = null;
        if (value instanceof Boolean) {
            type = AttributeDTO.EType.BOOLEAN;
        } else if (value instanceof Long) {
            type = AttributeDTO.EType.LONG;
        } else if (value instanceof String) {
            type = AttributeDTO.EType.STRING;
        } else if (value == null) {
            // skip
            return;
        } else {
            throw new IllegalArgumentException("Attribute value must be String, Boolean or Long and not: " + value.getClass().getName());
        }

        String sql = "INSERT INTO Attribute (entityId, entityKind, name, type, value) VALUES (?, ?, ?, ?, ?)";

        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ps.setLong(1, entityId);
            ps.setString(2, entityKind);
            ps.setString(3, name);
            ps.setInt(4, type.getType());
            ps.setString(5, value.toString());
            ps.execute();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException ex) {
                    Logger.getLogger(RelationalDB.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    private long getLastInsertedId(Statement statement) throws SQLException, DatabaseException {
        ResultSet rs = statement.getGeneratedKeys();
        try {
            if (rs.next()) {
                return rs.getLong(1);
            } else {
                throw new DatabaseException("Can't get last generated id.");
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }

    }

    private void update(Entity entity) {
        deleteAttributes(entity.getId());
        updateEntity(entity);
    }
    
    private void deleteAttributes(long entityId) {
        String attributeSql = "DELETE FROM Attribute WHERE entityId=" + entityId;

        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(attributeSql);
        } catch (SQLException ex) {
            Logger.getLogger(RelationalDB.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException("Deleting entity failed.", ex);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    Logger.getLogger(RelationalDB.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }
    
    private void updateEntity(Entity entity) {
        String sql = "UPDATE Entity SET kind=?, value=? WHERE id=" + entity.getId();
        
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);
            ps.setString(1, entity.getKind());
            if (entity.getValue() != null) {
                ps.setBlob(2, new SerialBlob(entity.getByteValue()));
            } else {
                ps.setNull(2, Types.BLOB);
            }
            ps.execute();

            for (Map.Entry<String, Object> entry : entity.getAttributes().entrySet()) {
                putAttribute(entity.getId(), entity.getKind(), entry.getKey(), entry.getValue());
            }

        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException ex) {
                    Logger.getLogger(RelationalDB.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    public void delete(long id) {
        String attributeSql = "DELETE FROM Attribute WHERE entityId=" + id;
        String entitySql = "DELETE FROM Entity WHERE id=" + id;

        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(attributeSql);
            stmt.execute(entitySql);
        } catch (SQLException ex) {
            Logger.getLogger(RelationalDB.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException("Deleting entity failed.", ex);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    Logger.getLogger(RelationalDB.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    public List<Entity> query(Filter filter) {
        String sql = "SELECT entityId FROM Attribute WHERE " + filter.getCondition();
        
        Set<Long> ids = new TreeSet<>();
        
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                ids.add(rs.getLong(1));
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    Logger.getLogger(RelationalDB.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    Logger.getLogger(RelationalDB.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        
        List<Entity> res = new ArrayList<>();
        Iterator<Long> it = ids.iterator();
        while (it.hasNext()) {
            Long id = it.next();
            Entity e = get(id);
            if (filter.match(e)) {
                res.add(e);
            }
        }
        
        return res;
    }

    public Set<Long> queryKeys(Filter filter) {
        String sql = "SELECT entityId FROM Attribute WHERE " + filter.getCondition();
        
        Set<Long> res = new TreeSet<>();
        
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                res.add(rs.getLong(1));
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    Logger.getLogger(RelationalDB.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    Logger.getLogger(RelationalDB.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        
        Iterator<Long> it = res.iterator();
        while (it.hasNext()) {
            Long id = it.next();
            Entity e = get(id);
            if (!filter.match(e)) {
                it.remove();
            }
        }
        
        return res;
    }

    public Entity queryFirst(Filter filter) {
        List<Entity> resList = query(filter);
        if (resList.size() < 1) {
            return null;
        } else {
            return resList.get(0);
        }
    }

    public Entity querySingleton(Filter filter) {
        List<Entity> resList = query(filter);
        if (resList.size() != 1) {
            throw new RuntimeException("Singleton result required but there are " + resList.size() + " results.");
        } else {
            return resList.get(0);
        }
    }

    private Map<String, AttributeDTO> getAttributes(long entityId) throws DatabaseException {
        Map<String, AttributeDTO> res = new HashMap<>();

        String sql = "SELECT * FROM Attribute WHERE entityId=" + entityId;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                AttributeDTO attr = createAttributeFromResultSet(rs);
                res.put(attr.getName(), attr);
            }
        } catch (SQLException ex) {
            throw new DatabaseException(ex);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    Logger.getLogger(RelationalDB.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        return res;
    }

    private AttributeDTO createAttributeFromResultSet(ResultSet rs) throws SQLException {
        AttributeDTO res = new AttributeDTO();
        res.setId(rs.getLong("id"));
        res.setEntityId(rs.getLong("entityId"));
        res.setName(rs.getString("name"));
        res.setValue(rs.getString("value"));
        res.setType(AttributeDTO.EType.getFromType(rs.getInt("type")));
        return res;
    }

    public boolean isClosed() {
        try {
            return conn.isClosed();
        } catch (SQLException ex) {
            Logger.getLogger(RelationalDB.class.getName()).log(Level.SEVERE, null, ex);
            return true;
        }
    }

    private void assertClosed() {
        if (isClosed()) {
            throw new RuntimeException("The database is closed.");
        }
    }

    static class AttributeDTO {

        static enum EType {

            LONG,
            BOOLEAN,
            STRING;

            public static EType getFromType(int type) {
                switch (type) {
                    case 0:
                        return EType.LONG;
                    case 1:
                        return EType.BOOLEAN;
                    case 2:
                        return EType.STRING;
                    default:
                        throw new IllegalArgumentException("Invalid attribute type: " + type);
                }
            }

            public int getType() {
                if (this == EType.LONG) {
                    return 0;
                } else if (this == EType.BOOLEAN) {
                    return 1;
                } else if (this == EType.STRING) {
                    return 2;
                } else {
                    throw new IllegalArgumentException("Unhandled data type.");
                }
            }
        }

        private long id;
        private long entityId;
        private EType type;
        private String name;
        private String value;

        public AttributeDTO() {
        }

        public AttributeDTO(long id, long entityId, EType type, String name, String value) {
            this.id = id;
            this.entityId = entityId;
            this.type = type;
            this.name = name;
            this.value = value;
        }

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public long getEntityId() {
            return entityId;
        }

        public void setEntityId(long entityId) {
            this.entityId = entityId;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public EType getType() {
            return type;
        }

        public void setType(EType type) {
            this.type = type;
        }

    }

}
