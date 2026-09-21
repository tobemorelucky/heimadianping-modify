package com.hmdp.controller;

import com.hmdp.dto.Result;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.web.MockMultipartFile;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UploadControllerTest {

    @TempDir
    Path uploadRoot;

    @Test
    void shouldSaveAndDeleteBlogImageUnderFrontendImageDirectory() throws Exception {
        UploadController controller = new UploadController(uploadRoot);
        MockMultipartFile image = new MockMultipartFile(
                "file", "note.png", "image/png", new byte[]{1, 2, 3});

        Result uploadResult = controller.uploadImage(image);
        String relativePath = (String) uploadResult.getData();
        Path storedFile = uploadRoot.resolve(relativePath.substring(1)).normalize();

        assertTrue(uploadResult.getSuccess());
        assertTrue(storedFile.startsWith(uploadRoot));
        assertTrue(Files.exists(storedFile));

        Result deleteResult = controller.deleteBlogImg("/imgs" + relativePath);

        assertTrue(deleteResult.getSuccess());
        assertFalse(Files.exists(storedFile));
    }
}
