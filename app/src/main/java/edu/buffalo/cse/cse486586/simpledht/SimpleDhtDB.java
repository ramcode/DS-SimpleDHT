package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

/**
 * Created by ramesh on 2/15/16.
 */
public class SimpleDhtDB extends SQLiteOpenHelper {

    public static String TAG = SQLiteOpenHelper.class.getSimpleName();

    static final String DB_NAME = "simpledht.db";
    static final String TABLE_NAME = "messages";
    private static Integer DB_VERSION = 2;
    static final String TABLE_COL_KEY = "key";
    static final String TABLE_COL_VAL = "value";
    private static final String CREATE_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS "+
                                            TABLE_NAME+" ( "+
                                            TABLE_COL_KEY+ " TEXT PRIMARY KEY, "+
                                            TABLE_COL_VAL+" TEXT NOT NULL );";
    private static final String DROP_TABLE_QUERY = " DROP TABLE IF EXISTS "+TABLE_NAME+" ;";

    /*
    Constructor for GroupMessengerDB
    @params context
    @init super params with context, DB_NAME, DB_VERSION, null value refers to default cursor
     */
    public SimpleDhtDB(Context context){
        super(context, DB_NAME, null, DB_VERSION);
        Log.d(TAG, "Initializing DB....");
    }
    @Override
    public void onCreate(SQLiteDatabase db) {
        try{
            Log.d(TAG, "Creating Table "+TABLE_NAME+": "+CREATE_TABLE_QUERY);
            db.execSQL(CREATE_TABLE_QUERY);
        }
        catch (Exception ex){
            Log.e(TAG, "Error in Creating Table: "+ex.getMessage());
        }

    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            db.execSQL(DROP_TABLE_QUERY);
            onCreate(db);
    }
}
