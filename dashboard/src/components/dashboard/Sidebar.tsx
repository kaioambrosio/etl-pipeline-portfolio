import { LayoutDashboard, Database } from "lucide-react";
import { cn } from "@/lib/utils";

const menuItems = [{ icon: LayoutDashboard, label: "Dashboard", active: true }];

export function Sidebar() {
  return (
    <aside className="fixed left-0 top-0 bottom-0 w-16 bg-sidebar flex flex-col items-center py-4 z-50">
      <div
        className="w-10 h-10 rounded-xl bg-primary flex items-center justify-center mb-8"
        title="ETL Portfólio"
      >
        <Database className="w-5 h-5 text-primary-foreground" />
      </div>

      <nav className="flex-1 flex flex-col items-center gap-2">
        {menuItems.map((item, index) => (
          <button
            key={index}
            className={cn("sidebar-icon-btn", item.active && "active")}
            title={item.label}
          >
            <item.icon className="w-5 h-5" />
          </button>
        ))}
      </nav>
    </aside>
  );
}
