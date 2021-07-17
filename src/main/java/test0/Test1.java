package test0;

import java.util.Date;

public class Test1 {
    public static void main(String[] args) {
        String strA=null;
        if(strA==null){
            System.out.println("True");
        }else if(strA.compareTo(null)>0){
            System.out.println("False");
        }

        System.out.println(new Date(0).toString());


    }
}
