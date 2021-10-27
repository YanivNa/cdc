package utility;

import org.json.JSONObject;

public class Utils {
    private Utils(){

    }

    public static boolean isFieldExist(String key, JSONObject jsonObject){
        return jsonObject.has(key) && null!=jsonObject.get(key);
    }
}
