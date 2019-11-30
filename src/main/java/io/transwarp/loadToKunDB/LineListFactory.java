package io.transwarp.loadToKunDB;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class LineListFactory extends BasePooledObjectFactory<LineList> {
    private final int listCap;
    private final int lineCap;

    public LineListFactory(int listCap, int lineCap){
        this.listCap = listCap;
        this.lineCap = lineCap;
    }

    @Override
    public LineList create() throws Exception {
        return new LineList(listCap, lineCap);
    }

    @Override
    public PooledObject<LineList> wrap(LineList lineList) {
        return new DefaultPooledObject<>(lineList);
    }

    @Override
    public void passivateObject(PooledObject<LineList> p) throws Exception {
        p.getObject().size = 0;
    }
}
