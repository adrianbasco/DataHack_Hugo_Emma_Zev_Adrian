import { Tabs } from "expo-router";
import { Ionicons } from "@expo/vector-icons";
import { View } from "react-native";
import { palette } from "../../components/ui";

export default function TabsLayout() {
  return (
    <Tabs
      screenOptions={{
        headerShown: false,
        tabBarShowLabel: false,
        tabBarStyle: {
          position: "absolute",
          left: 16,
          right: 16,
          bottom: 16,
          height: 72,
          borderRadius: 999,
          backgroundColor: palette.panel,
          borderTopWidth: 0,
          elevation: 10,
          shadowColor: "#000",
          shadowOpacity: 0.12,
          shadowRadius: 16,
          shadowOffset: { width: 0, height: 6 },
        },
        tabBarActiveTintColor: palette.accent,
        tabBarInactiveTintColor: palette.textMuted,
      }}
    >
      <Tabs.Screen
        name="index"
        options={{
          title: "Home",
          tabBarIcon: ({ color, focused }) => (
            <Ionicons
              name={focused ? "flame" : "flame-outline"}
              size={24}
              color={color}
            />
          ),
        }}
      />

      <Tabs.Screen
        name="saved"
        options={{
          title: "Saved",
          tabBarIcon: ({ focused }) => (
            <View
              style={{
                width: 56,
                height: 56,
                borderRadius: 28,
                backgroundColor: focused ? palette.accent : palette.panel,
                alignItems: "center",
                justifyContent: "center",
                marginTop: -18,
                borderWidth: 3,
                borderColor: "rgba(255,255,255,0.08)",
              }}
            >
              <Ionicons
                name={focused ? "heart" : "heart-outline"}
                size={26}
                color={focused ? "#08111f" : palette.text}
              />
            </View>
          ),
        }}
      />

      {/* <Tabs.Screen
        name="templates"
        options={{
          title: "Templates",
          tabBarIcon: ({ color, focused }) => (
            <Ionicons
              name={focused ? "grid" : "grid-outline"}
              size={24}
              color={color}
            /> */}
          {/* ), */}
        {/* }} */}
      {/* /> */}
    </Tabs>
  );
}