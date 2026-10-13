import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

const sidebar: SidebarsConfig = {
  apisidebar: [
    {
      type: "doc",
      id: "api-reference/rest-reports/process-path-management-reports-api",
    },
    {
      type: "category",
      label: "reports",
      link: {
        type: "doc",
        id: "api-reference/rest-reports/reports",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest-reports/get-catalogue-growth-report",
          label: "Process paths defined, revised and deactivated per UTC day",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest-reports/get-catalogue-growth-freshness",
          label: "Time since the most recently projected catalogue change",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "health",
      link: {
        type: "doc",
        id: "api-reference/rest-reports/health",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest-reports/get-reports-healthz",
          label: "Liveness probe",
          className: "api-method get",
        },
      ],
    },
  ],
};

export default sidebar.apisidebar;
