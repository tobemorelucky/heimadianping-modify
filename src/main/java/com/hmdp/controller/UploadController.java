package com.hmdp.controller;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.Result;
import com.hmdp.utils.SystemConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("upload")
public class UploadController {

    private final Path uploadRoot;

    public UploadController() {
        this(Paths.get(SystemConstants.IMAGE_UPLOAD_DIR));
    }

    /**
     * 允许测试使用隔离目录，正式运行仍使用项目前端图片目录。
     */
    UploadController(Path uploadRoot) {
        this.uploadRoot = uploadRoot.toAbsolutePath().normalize();
    }

    @PostMapping("blog")
    public Result uploadImage(@RequestParam("file") MultipartFile image) {
        try {
            // 获取原始文件名称
            String originalFilename = image.getOriginalFilename();
            // 生成新文件名
            String fileName = createNewFileName(originalFilename);
            // 保存文件
            image.transferTo(resolveUploadFile(fileName));
            // 返回结果
            log.debug("文件上传成功，{}", fileName);
            return Result.ok(fileName);
        } catch (IOException e) {
            throw new RuntimeException("文件上传失败", e);
        }
    }

    @GetMapping("/blog/delete")
    public Result deleteBlogImg(@RequestParam("name") String filename) {
        File file = resolveUploadFile(filename);
        if (file.isDirectory()) {
            return Result.fail("错误的文件名称");
        }
        FileUtil.del(file);
        return Result.ok();
    }

    private String createNewFileName(String originalFilename) {
        // 获取后缀
        String suffix = StrUtil.subAfter(originalFilename, ".", true);
        // 生成目录
        String name = UUID.randomUUID().toString();
        int hash = name.hashCode();
        int d1 = hash & 0xF;
        int d2 = (hash >> 4) & 0xF;
        // 判断目录是否存在
        File dir = resolveUploadFile(StrUtil.format("/blogs/{}/{}", d1, d2));
        if (!dir.exists()) {
            dir.mkdirs();
        }
        // 生成文件名
        return StrUtil.format("/blogs/{}/{}/{}.{}", d1, d2, name, suffix);
    }

    /**
     * 将前端使用的 /imgs/blogs/... 或上传接口返回的 /blogs/... 转成图片根目录下的文件，
     * 同时阻止通过相对路径访问图片目录之外的文件。
     */
    private File resolveUploadFile(String filename) {
        String relativeName = filename == null ? "" : filename.replace('\\', '/');
        if (relativeName.startsWith("/imgs/")) {
            relativeName = relativeName.substring("/imgs/".length());
        } else if (relativeName.startsWith("/")) {
            relativeName = relativeName.substring(1);
        }

        Path target = uploadRoot.resolve(relativeName).normalize();
        if (!target.startsWith(uploadRoot)) {
            throw new IllegalArgumentException("错误的文件名称");
        }
        return target.toFile();
    }
}
