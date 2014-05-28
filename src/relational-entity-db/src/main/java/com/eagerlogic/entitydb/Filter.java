package com.eagerlogic.entitydb;

/**
 *
 * @author dipacs
 */
public final class Filter {

    private final String kind;
    private final AFilterItem filterItem;

    public Filter(String kind, AFilterItem filterItem) {
        if (kind == null) {
            throw new NullPointerException("The kind parameter can not be null.");
        }
        this.kind = kind;

        this.filterItem = filterItem;
    }

    public String getKind() {
        return kind;
    }

    public AFilterItem getFilterItem() {
        return filterItem;
    }

    String getCondition() {
        String res = "(entityKind='" + kind.replace("'", "''") + "'";
        if (filterItem != null) {
            String cond = filterItem.getCondition();
            if (cond != null) {
                res += " AND " + cond;
            }
        }
        res += ")";
        return res;
    }

    boolean match(Entity entity) {
        if (!kind.equals(entity.getKind())) {
            return false;
        }
        return filterItem.match(entity);
    }

}
