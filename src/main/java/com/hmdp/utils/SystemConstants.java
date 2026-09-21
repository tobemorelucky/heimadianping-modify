package com.hmdp.utils;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SystemConstants {
    /**
     * 图片默认保存到当前项目的 Nginx 前端静态资源目录。
     * 可通过 HMDP_IMAGE_UPLOAD_DIR 覆盖，方便部署到不同机器。
     */
    public static final String IMAGE_UPLOAD_DIR = resolveImageUploadDir();
    public static final String USER_NICK_NAME_PREFIX = "user_";
    public static final int DEFAULT_PAGE_SIZE = 5;
    public static final int MAX_PAGE_SIZE = 10;

    private static String resolveImageUploadDir() {
        String configuredDir = System.getenv("HMDP_IMAGE_UPLOAD_DIR");
        Path uploadPath;
        if (configuredDir == null || configuredDir.trim().isEmpty()) {
            uploadPath = Paths.get(System.getProperty("user.dir"),
                    "nginx-1.18.0", "html", "hmdp", "imgs");
        } else {
            uploadPath = Paths.get(configuredDir);
        }
        return uploadPath.toAbsolutePath().normalize().toString() + File.separator;
    }
}
