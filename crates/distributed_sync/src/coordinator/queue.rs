use crate::coordinator::TaskPriority;
use crate::proto::TaskAssignment;
use std::collections::VecDeque;

const PRIORITY_LEVELS: usize = 5;

#[derive(Default)]
pub(super) struct TaskQueue {
    regular: [VecDeque<TaskAssignment>; PRIORITY_LEVELS],
    service: [VecDeque<TaskAssignment>; PRIORITY_LEVELS],
}

impl TaskQueue {
    pub(super) fn push(&mut self, assignment: TaskAssignment, priority: TaskPriority) {
        let queues = if assignment.service_task {
            &mut self.service
        } else {
            &mut self.regular
        };
        queues[priority_index(priority)].push_back(assignment);
    }

    pub(super) fn pop(&mut self, service_tasks_enabled: bool) -> Option<TaskAssignment> {
        if service_tasks_enabled && let Some(task) = pop_highest(&mut self.service) {
            return Some(task);
        }
        pop_highest(&mut self.regular)
    }

    pub(super) fn remove(&mut self, assignment_id: u64) {
        for queue in self.regular.iter_mut().chain(&mut self.service) {
            queue.retain(|task| task.assignment_id != assignment_id);
        }
    }

    pub(super) fn sizes(&self) -> (usize, usize) {
        (queue_size(&self.regular), queue_size(&self.service))
    }
}

fn priority_index(priority: TaskPriority) -> usize {
    match priority {
        TaskPriority::Lowest => 0,
        TaskPriority::Low => 1,
        TaskPriority::Normal => 2,
        TaskPriority::High => 3,
        TaskPriority::Highest => 4,
    }
}

fn pop_highest(queues: &mut [VecDeque<TaskAssignment>; PRIORITY_LEVELS]) -> Option<TaskAssignment> {
    queues.iter_mut().rev().find_map(VecDeque::pop_front)
}

fn queue_size(queues: &[VecDeque<TaskAssignment>; PRIORITY_LEVELS]) -> usize {
    queues.iter().map(VecDeque::len).sum()
}

#[cfg(test)]
mod tests {
    use super::TaskQueue;
    use crate::coordinator::TaskPriority;
    use crate::proto::TaskAssignment;

    fn assignment(id: u64, service_task: bool) -> TaskAssignment {
        TaskAssignment {
            assignment_id: id,
            handler_id: "test".to_owned(),
            payload: Vec::new(),
            timeout_ms: 1,
            service_task,
        }
    }

    #[test]
    fn test_priority_is_stable_and_service_workers_fall_back() {
        let mut queue = TaskQueue::default();
        queue.push(assignment(1, false), TaskPriority::Normal);
        queue.push(assignment(2, false), TaskPriority::Low);
        queue.push(assignment(3, false), TaskPriority::Lowest);
        queue.push(assignment(30, false), TaskPriority::Highest);
        queue.push(assignment(20, false), TaskPriority::High);
        queue.push(assignment(10, false), TaskPriority::High);
        queue.push(assignment(4, true), TaskPriority::Low);

        assert_eq!(queue.pop(false).map(|task| task.assignment_id), Some(30));
        assert_eq!(queue.pop(false).map(|task| task.assignment_id), Some(20));
        assert_eq!(queue.pop(false).map(|task| task.assignment_id), Some(10));
        assert_eq!(queue.pop(true).map(|task| task.assignment_id), Some(4));
        assert_eq!(queue.pop(true).map(|task| task.assignment_id), Some(1));
        assert_eq!(queue.pop(true).map(|task| task.assignment_id), Some(2));
        assert_eq!(queue.pop(true).map(|task| task.assignment_id), Some(3));
        assert!(queue.pop(true).is_none());
    }
}
