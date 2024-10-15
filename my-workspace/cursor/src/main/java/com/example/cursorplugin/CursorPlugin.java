package com.example.cursorplugin;

import com.intellij.openapi.project.Project;
import com.intellij.openapi.startup.StartupActivity;
import org.jetbrains.annotations.NotNull;

public class CursorPlugin implements StartupActivity {
    @Override
    public void runActivity(@NotNull Project project) {
        System.out.println("Cursor plugin is now active!");
    }
}