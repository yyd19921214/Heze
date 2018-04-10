import java.io.File;
import java.util.*;

public class Test {
    public static void main(String[] args) {
        int[] arr={2,1};
//        containsNearbyAlmostDuplicate(arr,1,1);
        findShortestSubArray(new int[]{1,2,2,3,1,4,2});
    }

    private int[] getArr(){
        return new int[]{1,1};
    }

    public static boolean containsNearbyAlmostDuplicate(int[] nums, int k, int t) {
        if (nums==null||nums.length==0)
            return false;
        TreeSet<Integer> treeSet=new TreeSet<>();
        for (int i=0;i<nums.length;i++){
            Integer floor=treeSet.floor(nums[i]+t);
            Integer ceil=treeSet.ceiling(nums[i]-t);
            if (floor!=null&&floor>=nums[i]||(ceil!=null&&ceil<=nums[i])){
                return true;
            }
            treeSet.add(nums[i]);
            if (i>=k){
                treeSet.remove(nums[i-k]);
            }
        }
        return false;
    }

    public static int findShortestSubArray(int[] nums) {
        Map<Integer,Integer> degree=new HashMap<>();
        Map<Integer,Integer> first=new HashMap<>();
        Map<Integer,Integer> end=new HashMap<>();
        for (int i=0;i<nums.length;i++){
            int t=nums[i];
            degree.put(t,degree.getOrDefault(t,0)+1);
            if (!first.containsKey(t)){
                first.put(t,i);
            }

            end.put(t,i);
        }
        int max=Integer.MIN_VALUE;
        int minLen=Integer.MAX_VALUE;
        for (int k:degree.keySet()){
            if (degree.get(k)>=max){
                if (end.get(k)-first.get(k)+1<minLen){
                    minLen=end.get(k)-first.get(k)+1;
                }
                max=degree.get(k);
            }
        }
        return minLen;
    }
}




