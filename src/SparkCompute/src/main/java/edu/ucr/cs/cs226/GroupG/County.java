package edu.ucr.cs.cs226.GroupG;

import com.vividsolutions.jts.geom.Geometry;

import java.io.Serializable;
import java.util.ArrayList;

public class County implements Serializable {
    ArrayList<Geometry> geom;
    ArrayList<String> name;

    public County(ArrayList<Geometry> geom, ArrayList<String> name) {
        this.geom = geom;
        this.name = name;
    }
}
