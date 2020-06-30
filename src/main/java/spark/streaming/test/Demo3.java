package spark.streaming.test;

/**
 * create by liuzhiwei on 2020/6/6
 * <p>
 *           10
 *      5         16
 *  4    7     13   17
 *     6  9  11
 *
 *
 *           10
 *        5     11
 *   4    7
 *      6  9
 *
 *
 *
 *  *        16
 *  *     13   17
 *
 *  *
 *
 *
 *
 *
 *
 *
 *
 *
 * <p>
 * <p>
 * 12  8
 * <p>
 * <p>
 * 2 -3 1 3 4 -1 5 -6
 */


public class Demo3 {
    public static int maxArray(int[] nums) {
        if (nums == null || nums.length == 0) {
            return -1;
        }
        int max = Integer.MIN_VALUE;
        int cur = 0;
        for (int i = 0; i < nums.length; i++) {

            cur = Math.max(nums[i], cur + nums[i]);

            max = Math.max(max, cur);
        }
        return max;
    }

    public static void main(String[] args) {
        int res= maxArray(new int[]{2, -3, 1, 3, 4, -1, 5, -6});
        System.out.println(res);
    }


}
