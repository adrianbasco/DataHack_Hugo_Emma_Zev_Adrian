import { useState } from "react";
import { StyleSheet, TextInput, View } from "react-native";

import { DateTemplate, GenerateChatRequest } from "../lib/types";
import { buildChatPlannerRequest } from "../lib/chatPlanner";
import { ActionButton, SurfaceCard, palette } from "./ui";

type Props = {
  selectedTemplate?: DateTemplate;
  onSubmit: (payload: GenerateChatRequest) => void;
};

export default function ChatPlanner({ selectedTemplate, onSubmit }: Props) {
  const [draft, setDraft] = useState("");

  function handleSubmit() {
    const normalizedDraft = draft.trim();
    if (!normalizedDraft) {
      return;
    }

    onSubmit(
      buildChatPlannerRequest({
        prompt: normalizedDraft,
        selectedTemplate,
      })
    );
  }

  return (
    <View style={styles.container}>
      <SurfaceCard style={styles.chatCard}>
        <TextInput
          style={[styles.input, styles.notesInput]}
          value={draft}
          onChangeText={setDraft}
          placeholder="Describe the date you want."
          placeholderTextColor={palette.textMuted}
          multiline
        />

        <ActionButton
          label="Generate ideas"
          onPress={handleSubmit}
          disabled={!draft.trim()}
        />
      </SurfaceCard>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    gap: 16,
  },
  chatCard: {
    gap: 14,
  },
  input: {
    borderWidth: 1,
    borderColor: palette.border,
    backgroundColor: palette.panelSoft,
    borderRadius: 18,
    paddingHorizontal: 16,
    paddingVertical: 14,
    fontSize: 15,
    color: palette.text,
  },
  notesInput: {
    minHeight: 160,
    textAlignVertical: "top",
  },
});
