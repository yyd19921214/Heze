package com.yudy.heze.zk;

import java.util.List;

public interface ZkChildListener {

    public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception;
}
