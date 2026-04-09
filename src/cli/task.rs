use std::path::PathBuf;

use clap::{Args, Subcommand};

use crate::shared::error::AppResult;

use super::project::{self, CreateFromBeadArgs, ProjectCommand, ProjectSubcommand};

/// Task commands — aliases for project operations in a bead/milestone context.
///
/// A "task" is a project that was created from a bead. These commands provide
/// a task-oriented vocabulary while delegating to the underlying project services.
#[derive(Debug, Args)]
#[command(about = "Task commands (aliases for bead-backed project operations)")]
pub struct TaskCommand {
    #[command(subcommand)]
    pub command: TaskSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum TaskSubcommand {
    /// Create a new task from a bead (wraps `project create-from-bead`)
    Create(TaskCreateArgs),
    /// Show task details (wraps `project show`)
    Show {
        /// Task/project ID. Defaults to the active project if omitted.
        id: Option<String>,
    },
    /// Select the active task (wraps `project select`)
    Select {
        /// Task/project ID to make active
        id: String,
    },
    /// List all tasks/projects (wraps `project list`)
    List,
}

#[derive(Debug, Args)]
pub struct TaskCreateArgs {
    #[arg(long = "milestone-id")]
    pub milestone_id: String,
    #[arg(long = "bead-id")]
    pub bead_id: String,
    #[arg(long = "project-id")]
    pub project_id: Option<String>,
    #[arg(long = "prompt-file")]
    pub prompt_file: Option<PathBuf>,
    #[arg(long)]
    pub flow: Option<String>,
}

pub async fn handle(command: TaskCommand) -> AppResult<()> {
    match command.command {
        TaskSubcommand::Create(args) => {
            let project_cmd = ProjectCommand {
                command: ProjectSubcommand::CreateFromBead(CreateFromBeadArgs {
                    milestone_id: args.milestone_id,
                    bead_id: args.bead_id,
                    project_id: args.project_id,
                    prompt_file: args.prompt_file,
                    flow: args.flow,
                }),
            };
            project::handle(project_cmd).await
        }
        TaskSubcommand::Show { id } => {
            let project_cmd = ProjectCommand {
                command: ProjectSubcommand::Show { id },
            };
            project::handle(project_cmd).await
        }
        TaskSubcommand::Select { id } => {
            let project_cmd = ProjectCommand {
                command: ProjectSubcommand::Select { id },
            };
            project::handle(project_cmd).await
        }
        TaskSubcommand::List => {
            let project_cmd = ProjectCommand {
                command: ProjectSubcommand::List,
            };
            project::handle(project_cmd).await
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::cli::{Cli, Commands};

    use super::*;

    /// Verify `task create` parses into the expected TaskSubcommand::Create variant.
    #[test]
    fn task_create_parses_correctly() -> Result<(), Box<dyn std::error::Error>> {
        let cli = Cli::parse_from([
            "ralph-burning",
            "task",
            "create",
            "--milestone-id",
            "m1",
            "--bead-id",
            "b1",
        ]);
        let Commands::Task(task_cmd) = cli.command else {
            panic!("expected Task command");
        };
        let TaskSubcommand::Create(args) = task_cmd.command else {
            panic!("expected Create subcommand");
        };
        assert_eq!(args.milestone_id, "m1");
        assert_eq!(args.bead_id, "b1");
        assert!(args.project_id.is_none());
        assert!(args.prompt_file.is_none());
        assert!(args.flow.is_none());
        Ok(())
    }

    /// Verify `task create` accepts optional --project-id, --prompt-file, --flow.
    #[test]
    fn task_create_with_all_optional_args() -> Result<(), Box<dyn std::error::Error>> {
        let cli = Cli::parse_from([
            "ralph-burning",
            "task",
            "create",
            "--milestone-id",
            "m1",
            "--bead-id",
            "b1",
            "--project-id",
            "custom-id",
            "--prompt-file",
            "/tmp/prompt.md",
            "--flow",
            "minimal",
        ]);
        let Commands::Task(task_cmd) = cli.command else {
            panic!("expected Task command");
        };
        let TaskSubcommand::Create(args) = task_cmd.command else {
            panic!("expected Create subcommand");
        };
        assert_eq!(args.project_id.as_deref(), Some("custom-id"));
        assert_eq!(
            args.prompt_file.as_deref(),
            Some(std::path::Path::new("/tmp/prompt.md"))
        );
        assert_eq!(args.flow.as_deref(), Some("minimal"));
        Ok(())
    }

    /// Verify `task show` parses with optional ID.
    #[test]
    fn task_show_parses_without_id() -> Result<(), Box<dyn std::error::Error>> {
        let cli = Cli::parse_from(["ralph-burning", "task", "show"]);
        let Commands::Task(task_cmd) = cli.command else {
            panic!("expected Task command");
        };
        assert!(matches!(
            task_cmd.command,
            TaskSubcommand::Show { id: None }
        ));
        Ok(())
    }

    #[test]
    fn task_show_parses_with_id() -> Result<(), Box<dyn std::error::Error>> {
        let cli = Cli::parse_from(["ralph-burning", "task", "show", "my-task"]);
        let Commands::Task(task_cmd) = cli.command else {
            panic!("expected Task command");
        };
        let TaskSubcommand::Show { id } = task_cmd.command else {
            panic!("expected Show subcommand");
        };
        assert_eq!(id.as_deref(), Some("my-task"));
        Ok(())
    }

    /// Verify `task select` parses the required ID argument.
    #[test]
    fn task_select_parses_id() -> Result<(), Box<dyn std::error::Error>> {
        let cli = Cli::parse_from(["ralph-burning", "task", "select", "my-task"]);
        let Commands::Task(task_cmd) = cli.command else {
            panic!("expected Task command");
        };
        let TaskSubcommand::Select { id } = task_cmd.command else {
            panic!("expected Select subcommand");
        };
        assert_eq!(id, "my-task");
        Ok(())
    }

    /// Verify `task list` parses correctly.
    #[test]
    fn task_list_parses() -> Result<(), Box<dyn std::error::Error>> {
        let cli = Cli::parse_from(["ralph-burning", "task", "list"]);
        let Commands::Task(task_cmd) = cli.command else {
            panic!("expected Task command");
        };
        assert!(matches!(task_cmd.command, TaskSubcommand::List));
        Ok(())
    }

    /// Verify that `task create` builds a CreateFromBeadArgs that matches what
    /// `project create-from-bead` would receive with the same arguments.
    #[test]
    fn task_create_maps_to_project_create_from_bead_args() -> Result<(), Box<dyn std::error::Error>>
    {
        // Parse the task form
        let task_cli = Cli::parse_from([
            "ralph-burning",
            "task",
            "create",
            "--milestone-id",
            "ms1",
            "--bead-id",
            "bd1",
            "--project-id",
            "p1",
            "--flow",
            "minimal",
        ]);
        // Parse the project form
        let project_cli = Cli::parse_from([
            "ralph-burning",
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms1",
            "--bead-id",
            "bd1",
            "--project-id",
            "p1",
            "--flow",
            "minimal",
        ]);

        let Commands::Task(task_cmd) = task_cli.command else {
            panic!("expected Task command");
        };
        let TaskSubcommand::Create(task_args) = task_cmd.command else {
            panic!("expected Create subcommand");
        };
        let Commands::Project(project_cmd) = project_cli.command else {
            panic!("expected Project command");
        };
        let ProjectSubcommand::CreateFromBead(project_args) = project_cmd.command else {
            panic!("expected CreateFromBead subcommand");
        };

        assert_eq!(task_args.milestone_id, project_args.milestone_id);
        assert_eq!(task_args.bead_id, project_args.bead_id);
        assert_eq!(task_args.project_id, project_args.project_id);
        assert_eq!(task_args.flow, project_args.flow);
        Ok(())
    }

    /// Verify `task select` rejects missing ID (clap validation).
    #[test]
    fn task_select_rejects_missing_id() {
        let result = Cli::try_parse_from(["ralph-burning", "task", "select"]);
        assert!(result.is_err());
    }

    /// Verify `task create` rejects missing required args (clap validation).
    #[test]
    fn task_create_rejects_missing_required_args() {
        let result = Cli::try_parse_from(["ralph-burning", "task", "create"]);
        assert!(result.is_err());
    }
}
