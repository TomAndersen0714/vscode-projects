package com.example.cursorplugin;

import com.intellij.lang.Language;
import com.intellij.lang.injection.MultiHostInjector;
import com.intellij.lang.injection.MultiHostRegistrar;
import com.intellij.openapi.util.TextRange;
import com.intellij.psi.PsiElement;
import com.intellij.psi.PsiLanguageInjectionHost;
import com.intellij.psi.json.JsonProperty;
import com.intellij.psi.json.JsonStringLiteral;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;

public class SqlInJsonLanguageInjector implements MultiHostInjector {
    @Override
    public void getLanguagesToInject(@NotNull MultiHostRegistrar registrar, @NotNull PsiElement context) {
        if (!(context instanceof JsonStringLiteral)) return;

        PsiElement parent = context.getParent();
        if (!(parent instanceof JsonProperty)) return;

        JsonProperty property = (JsonProperty) parent;
        if (!"sql".equals(property.getName())) return;

        Language sqlLanguage = Language.findLanguageByID("SQL");
        if (sqlLanguage == null) return;

        String text = ((JsonStringLiteral) context).getValue();
        registrar.startInjecting(sqlLanguage)
                .addPlace("", "", (PsiLanguageInjectionHost) context, TextRange.from(1, text.length()))
                .doneInjecting();
    }

    @NotNull
    @Override
    public List<? extends Class<? extends PsiElement>> elementsToInjectIn() {
        return Collections.singletonList(JsonStringLiteral.class);
    }
}