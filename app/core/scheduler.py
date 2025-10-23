import logging
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.asyncio import AsyncIOExecutor

from app.jobs import movie_discovery_job, change_tracking_job, category_refresh_job

logger = logging.getLogger(__name__)


class JobScheduler:
    """Manages all scheduled jobs for SAGEPICK."""
    
    def __init__(self):
        self._job_ids = {
            'movie_discovery_job',
            'change_tracking_job',
            'category_refresh_job'
        }
        self._create_scheduler()

    def _create_scheduler(self):
        """Create a fresh scheduler instance with default configuration."""
        jobstores = {
            'default': MemoryJobStore()
        }

        executors = {
            'default': AsyncIOExecutor()
        }

        job_defaults = {
            'coalesce': True,
            'max_instances': 1,
            'misfire_grace_time': 30
        }

        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone='UTC'
        )
        self._jobs_configured = False
    
    def configure_jobs(self):
        """Configure all scheduled jobs."""
        if self._jobs_configured:
            missing_jobs = [job_id for job_id in self._job_ids if not self.scheduler.get_job(job_id)]
            if not missing_jobs:
                logger.debug("Jobs already configured, skipping configuration")
                return
            logger.warning(
                "Detected missing jobs on scheduler restart, reconfiguring: %s",
                ", ".join(missing_jobs)
            )
        
        try:
            # Job 1: Movie Discovery Job - Every 5 minutes
            self.scheduler.add_job(
                func=movie_discovery_job.run,
                trigger=IntervalTrigger(minutes=5),
                id='movie_discovery_job',
                name='Movie Discovery Job',
                replace_existing=True,
                next_run_time=datetime.utcnow() + timedelta(hours=10)  # start after 10 hours from now
            )
            logger.info("Configured Movie Discovery Job - runs every 5 minutes")
            
            # Job 2: Change Tracking Job - Daily at 2:00 AM UTC
            self.scheduler.add_job(
                func=change_tracking_job.run,
                trigger=CronTrigger(hour=2, minute=0),
                id='change_tracking_job', 
                name='Change Tracking Job',
                replace_existing=True
            )
            logger.info("Configured Change Tracking Job - runs daily at 2:00 AM UTC")
            
            # Job 3: Category Refresh Job - Daily at 5:00 AM UTC
            self.scheduler.add_job(
                func=category_refresh_job.run,
                trigger=CronTrigger(hour=5, minute=0),
                id='category_refresh_job',
                name='Category Refresh Job', 
                replace_existing=True
            )
            logger.info("Configured Category Refresh Job - runs daily at 5:00 AM UTC")
            
            self._jobs_configured = True
            logger.info("All jobs configured successfully")
            
        except Exception as e:
            logger.error(f"Failed to configure jobs: {str(e)}")
            raise
    
    async def start(self):
        """Start the scheduler."""
        try:
            if self.scheduler.running:
                logger.info("Job scheduler already running")
                return

            self.configure_jobs()
            
            self.scheduler.start()
            logger.info("Job scheduler started successfully")
            
            # Log next run times
            for job in self.scheduler.get_jobs():
                logger.info(f"Job '{job.name}' next run: {job.next_run_time}")
                
        except Exception as e:
            logger.error(f"Failed to start scheduler: {str(e)}")
            raise
    
    async def stop(self):
        """Stop the scheduler."""
        try:
            if self.scheduler.running:
                self.scheduler.shutdown(wait=True)
                logger.info("Job scheduler stopped successfully")
            else:
                logger.info("Job scheduler already stopped")
        except Exception as e:
            logger.error(f"Failed to stop scheduler: {str(e)}")
            raise
        finally:
            # Reset the scheduler so a subsequent start creates fresh jobs
            self._create_scheduler()
    
    def get_job_status(self, job_id: str) -> dict:
        """Get status of a specific job."""
        job = self.scheduler.get_job(job_id)
        if job:
            return {
                "id": job.id,
                "name": job.name,
                "next_run_time": job.next_run_time,
                "trigger": str(job.trigger),
                "func": job.func.__name__ if job.func else None
            }
        return None
    
    def get_all_jobs_status(self) -> list:
        """Get status of all scheduled jobs."""
        jobs_status = []
        for job in self.scheduler.get_jobs():
            jobs_status.append({
                "id": job.id,
                "name": job.name,
                "next_run_time": job.next_run_time,
                "trigger": str(job.trigger),
                "func": job.func.__name__ if job.func else None
            })
        return jobs_status
    
    async def trigger_job_manually(self, job_id: str) -> bool:
        """Manually trigger a job to run immediately."""
        try:
            job = self.scheduler.get_job(job_id)
            if job:
                # Run the job function directly since we want it async
                if job_id == 'movie_discovery_job':
                    await movie_discovery_job.run()
                elif job_id == 'change_tracking_job':
                    await change_tracking_job.run()
                elif job_id == 'category_refresh_job':
                    await category_refresh_job.run()
                else:
                    return False
                
                logger.info(f"Manually triggered job: {job_id}")
                return True
            else:
                logger.error(f"Job not found: {job_id}")
                return False
        except Exception as e:
            logger.error(f"Failed to trigger job {job_id}: {str(e)}")
            return False
    
    def pause_job(self, job_id: str) -> bool:
        """Pause a specific job."""
        try:
            self.scheduler.pause_job(job_id)
            logger.info(f"Paused job: {job_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to pause job {job_id}: {str(e)}")
            return False
    
    def resume_job(self, job_id: str) -> bool:
        """Resume a specific job."""
        try:
            self.scheduler.resume_job(job_id)
            logger.info(f"Resumed job: {job_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to resume job {job_id}: {str(e)}")
            return False
    
    @property
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self.scheduler.running


# Global scheduler instance
job_scheduler = JobScheduler()