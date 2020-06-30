package spark.streaming.test;

import java.util.ArrayList;
import java.util.List;

/**
 * create by liuzhiwei on 2020/6/6
 */

class TreeNode {
    int val;
    TreeNode left;
    TreeNode right;

    public TreeNode(int val) {
        this.val = val;
    }
}

public class Demo2 {
    public List<Integer> leftView(TreeNode root) {
        List<Integer> res = new ArrayList<>();
        if (root == null) {
            return res;
        }
        help(root, res, 0);
        return res;
    }

    private void help(TreeNode root, List<Integer> res, int level) {
        if (root == null) {
            return;
        }
        if (level == res.size()) {
            res.add(root.val);
        }
        help(root.left, res, level + 1);
        help(root.right, res, level + 1);
    }
}
